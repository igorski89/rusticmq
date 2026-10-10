//! Queue definition files and transactional SQLite message storage.
use crate::{QueueConfig, QueueEvent, QueueManager, config};
use rusqlite::{Connection, params};
use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
};

#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Queue(#[from] crate::Error),
    #[error("{0}")]
    Config(String),
}

pub struct Persistence {
    definitions_path: Option<PathBuf>,
    connection: Option<Mutex<Connection>>,
}

impl Persistence {
    pub fn open(
        definitions_path: Option<PathBuf>,
        database_path: Option<PathBuf>,
        prune: bool,
    ) -> Result<Self, PersistenceError> {
        let connection = database_path.map(|path| -> Result<_, PersistenceError> {
            let connection = Connection::open(if path == Path::new(":memory") { PathBuf::from(":memory:") } else { path })?;
            connection.execute_batch("PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
                CREATE TABLE IF NOT EXISTS queues (name TEXT PRIMARY KEY, data TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS messages (queue TEXT NOT NULL REFERENCES queues(name) ON DELETE CASCADE,
                    id TEXT NOT NULL, data TEXT NOT NULL, PRIMARY KEY(queue,id));")?;
            if prune { connection.execute("DELETE FROM queues", [])?; }
            Ok(Mutex::new(connection))
        }).transpose()?;
        Ok(Self {
            definitions_path,
            connection,
        })
    }

    pub fn restore(
        &self,
        base: Vec<QueueConfig>,
        now: i64,
    ) -> Result<QueueManager, PersistenceError> {
        self.restore_with_limits(base, now, false)
    }

    pub fn restore_with_limits(
        &self,
        base: Vec<QueueConfig>,
        now: i64,
        relaxed_limits: bool,
    ) -> Result<QueueManager, PersistenceError> {
        let mut definitions: BTreeMap<_, _> = base
            .into_iter()
            .map(|queue| (queue.name.clone(), queue))
            .collect();
        if let Some(path) = &self.definitions_path
            && path.exists()
        {
            for queue in config::read_queue_file(path, now)
                .map_err(|error| PersistenceError::Config(error.to_string()))?
            {
                definitions.insert(queue.name.clone(), queue);
            }
        }
        if let Some(connection) = &self.connection {
            let connection = connection
                .lock()
                .map_err(|_| PersistenceError::Config("Database mutex poisoned".into()))?;
            let mut query = connection.prepare("SELECT data FROM queues ORDER BY name")?;
            let rows = query.query_map([], |row| row.get::<_, String>(0))?;
            for row in rows {
                let queue: QueueConfig = serde_json::from_str(&row?)?;
                definitions.insert(queue.name.clone(), queue);
            }
        }
        let mut manager = QueueManager::new();
        manager.set_relaxed_limits(relaxed_limits);
        manager.restore_queues(definitions.into_values().collect())?;
        if let Some(connection) = &self.connection {
            let connection = connection
                .lock()
                .map_err(|_| PersistenceError::Config("Database mutex poisoned".into()))?;
            let mut query =
                connection.prepare("SELECT queue,data FROM messages ORDER BY queue,id")?;
            let rows = query.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            for row in rows {
                let (queue, data) = row?;
                manager.restore_messages(&queue, vec![serde_json::from_str(&data)?])?;
            }
        }
        Ok(manager)
    }

    /// Events from one serialized engine operation form one SQL transaction.
    /// File persistence contains definitions only, matching upstream's file mode.
    pub fn apply(
        &self,
        events: &[QueueEvent],
        manager: &QueueManager,
    ) -> Result<(), PersistenceError> {
        if events.is_empty() {
            return Ok(());
        }
        if let Some(connection) = &self.connection {
            let mut connection = connection
                .lock()
                .map_err(|_| PersistenceError::Config("Database mutex poisoned".into()))?;
            let transaction = connection.transaction()?;
            for event in events {
                match event {
                    QueueEvent::QueueCreated(queue) | QueueEvent::QueueMetadataUpdated(queue) => {
                        transaction.execute("INSERT INTO queues(name,data) VALUES(?1,?2) ON CONFLICT(name) DO UPDATE SET data=excluded.data",
                            params![queue.name, serde_json::to_string(queue)?])?;
                    }
                    QueueEvent::QueueDeleted(name) => {
                        transaction.execute("DELETE FROM queues WHERE name=?1", [name])?;
                    }
                    QueueEvent::MessageAdded { queue, message }
                    | QueueEvent::MessageUpdated { queue, message } => {
                        transaction.execute("INSERT INTO messages(queue,id,data) VALUES(?1,?2,?3) ON CONFLICT(queue,id) DO UPDATE SET data=excluded.data",
                            params![queue,message.id,serde_json::to_string(message)?])?;
                    }
                    QueueEvent::MessageRemoved { queue, id } => {
                        transaction.execute(
                            "DELETE FROM messages WHERE queue=?1 AND id=?2",
                            params![queue, id],
                        )?;
                    }
                }
            }
            transaction.commit()?;
        }
        if let Some(path) = &self.definitions_path
            && events.iter().any(|event| {
                matches!(
                    event,
                    QueueEvent::QueueCreated(_)
                        | QueueEvent::QueueDeleted(_)
                        | QueueEvent::QueueMetadataUpdated(_)
                )
            })
        {
            let mut queues = serde_json::Map::new();
            for name in manager.list_queues(None) {
                let queue = manager.queue(&name)?;
                let mut data = serde_json::json!({
                    "defaultVisibilityTimeout":queue.visibility_timeout_ms,"delay":queue.delay_ms,
                    "receiveMessageWait":queue.receive_wait_ms,"fifo":queue.fifo,
                    "contentBasedDeduplication":queue.content_based_deduplication,"tags":queue.tags,
                    "created":queue.created,"lastModified":queue.last_modified,
                    "copyTo":queue.copy_messages_to.as_deref().unwrap_or(""),
                    "moveTo":queue.move_messages_to.as_deref().unwrap_or(""),
                });
                if let Some(dlq) = &queue.dead_letter_queue {
                    data["deadLettersQueue"] = serde_json::json!({"name":dlq.name,"maxReceiveCount":dlq.max_receive_count});
                }
                queues.insert(name, data);
            }
            atomic_write(
                path,
                &serde_json::to_vec_pretty(&serde_json::json!({"queues":queues}))?,
            )?;
        }
        Ok(())
    }
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let temporary = parent.join(format!(".rusticmq-{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        #[cfg(unix)]
        fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}
