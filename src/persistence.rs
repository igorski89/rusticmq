//! Queue definition files and transactional SQLite message storage.
use crate::{Message, QueueConfig, QueueEvent, QueueManager, config};
use rusqlite::{Connection, params};
use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

/// Limits on one group commit. A single job may exceed them on its own.
const MAX_BATCH_EVENTS: usize = 1024;
const MAX_BATCH_BODY_BYTES: usize = 64 * 1024 * 1024;
/// How often a failed group is retried when no new request arrives.
const RETRY_INTERVAL: Duration = Duration::from_millis(250);
const FAILURE_LOG_INTERVAL: Duration = Duration::from_secs(1);

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
            // `messages` holds each message as sent. `message_state` holds the fields
            // that receives and visibility changes update, so those never rewrite
            // the body. Version 0 databases have no `message_state` and need nothing
            // else; the table is created empty for them.
            connection.execute_batch("PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
                CREATE TABLE IF NOT EXISTS queues (name TEXT PRIMARY KEY, data TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS messages (queue TEXT NOT NULL REFERENCES queues(name) ON DELETE CASCADE,
                    id TEXT NOT NULL, data TEXT NOT NULL, PRIMARY KEY(queue,id));
                CREATE TABLE IF NOT EXISTS message_state (queue TEXT NOT NULL, id TEXT NOT NULL,
                    next_delivery INTEGER NOT NULL, receive_count INTEGER NOT NULL,
                    first_receive INTEGER, receipt_handle TEXT, PRIMARY KEY(queue,id),
                    FOREIGN KEY(queue,id) REFERENCES messages(queue,id) ON DELETE CASCADE);
                PRAGMA user_version=1;")?;
            if prune { connection.execute("DELETE FROM queues", [])?; }
            Ok(Mutex::new(connection))
        }).transpose()?;
        Ok(Self {
            definitions_path,
            connection,
        })
    }

    /// Whether messages are stored in SQLite, on disk or in memory.
    pub fn has_database(&self) -> bool {
        self.connection.is_some()
    }

    /// Whether anything is persisted at all: a database or a queue
    /// definitions file.
    pub fn writes_anything(&self) -> bool {
        self.connection.is_some() || self.definitions_path.is_some()
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
            let mut query = connection.prepare(
                "SELECT m.queue, m.data, s.next_delivery, s.receive_count, s.first_receive, s.receipt_handle
                 FROM messages m LEFT JOIN message_state s ON s.queue = m.queue AND s.id = m.id
                 ORDER BY m.queue, m.id",
            )?;
            let rows = query.query_map([], |row| {
                let state = match row.get::<_, Option<i64>>(2)? {
                    Some(next_delivery) => Some(MessageState {
                        next_delivery,
                        receive_count: row.get(3)?,
                        first_receive: row.get(4)?,
                        receipt_handle: row.get(5)?,
                    }),
                    None => None,
                };
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, state))
            })?;
            for row in rows {
                let (queue, data, state) = row?;
                let mut message: Message = serde_json::from_str(&data)?;
                if let Some(state) = state {
                    message.next_delivery = state.next_delivery;
                    message.receive_count = state.receive_count;
                    message.first_receive = state.first_receive;
                    message.receipt_handle = state.receipt_handle;
                }
                manager.restore_messages(&queue, vec![message])?;
            }
        }
        Ok(manager)
    }

    /// Commit events synchronously as one SQL transaction. The server commits
    /// through [`Writer`] instead, which groups concurrent requests.
    /// File persistence contains definitions only, matching upstream's file mode.
    pub fn apply(
        &self,
        events: &[QueueEvent],
        manager: &QueueManager,
    ) -> Result<(), PersistenceError> {
        let definitions = self.definitions(events, manager)?;
        self.commit(events, definitions.as_deref())
    }

    fn commit(
        &self,
        events: &[QueueEvent],
        definitions: Option<&[u8]>,
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
                        transaction
                            .prepare_cached("INSERT INTO queues(name,data) VALUES(?1,?2) ON CONFLICT(name) DO UPDATE SET data=excluded.data")?
                            .execute(params![queue.name, serde_json::to_string(queue)?])?;
                    }
                    QueueEvent::QueueDeleted(name) => {
                        transaction
                            .prepare_cached("DELETE FROM queues WHERE name=?1")?
                            .execute([name])?;
                    }
                    QueueEvent::MessageAdded { queue, message } => {
                        transaction
                            .prepare_cached("INSERT INTO messages(queue,id,data) VALUES(?1,?2,?3) ON CONFLICT(queue,id) DO UPDATE SET data=excluded.data")?
                            .execute(params![queue, message.id, serde_json::to_string(message)?])?;
                        // A replaced message starts from the state stored with it.
                        transaction
                            .prepare_cached("DELETE FROM message_state WHERE queue=?1 AND id=?2")?
                            .execute(params![queue, message.id])?;
                    }
                    // Receives and visibility changes only change these fields.
                    QueueEvent::MessageUpdated { queue, message } => {
                        transaction
                            .prepare_cached("INSERT INTO message_state(queue,id,next_delivery,receive_count,first_receive,receipt_handle)
                                VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(queue,id) DO UPDATE SET
                                next_delivery=excluded.next_delivery, receive_count=excluded.receive_count,
                                first_receive=excluded.first_receive, receipt_handle=excluded.receipt_handle")?
                            .execute(params![
                                queue,
                                message.id,
                                message.next_delivery,
                                message.receive_count,
                                message.first_receive,
                                message.receipt_handle
                            ])?;
                    }
                    QueueEvent::MessageRemoved { queue, id } => {
                        transaction
                            .prepare_cached("DELETE FROM messages WHERE queue=?1 AND id=?2")?
                            .execute(params![queue, id])?;
                    }
                }
            }
            transaction.commit()?;
        }
        if let (Some(path), Some(definitions)) = (&self.definitions_path, definitions) {
            atomic_write(path, definitions)?;
        }
        Ok(())
    }

    /// The definitions file contents, when these events change queue definitions.
    /// Taken while the caller holds the manager, so that commits can happen later.
    fn definitions(
        &self,
        events: &[QueueEvent],
        manager: &QueueManager,
    ) -> Result<Option<Vec<u8>>, PersistenceError> {
        if self.definitions_path.is_none()
            || !events.iter().any(|event| {
                matches!(
                    event,
                    QueueEvent::QueueCreated(_)
                        | QueueEvent::QueueDeleted(_)
                        | QueueEvent::QueueMetadataUpdated(_)
                )
            })
        {
            return Ok(None);
        }
        let mut queues = serde_json::Map::new();
        for name in manager.list_queues(None) {
            let queue = manager.queue(&name)?;
            // Written with the 2.0 keys; durations are bare milliseconds.
            let mut data = serde_json::json!({
                "default-visibility-timeout":queue.visibility_timeout_ms,"delay":queue.delay_ms,
                "receive-message-wait":queue.receive_wait_ms,"fifo":queue.fifo,
                "content-based-deduplication":queue.content_based_deduplication,"tags":queue.tags,
                "created":queue.created,"last-modified":queue.last_modified,
                "copy-to":queue.copy_messages_to.as_deref().unwrap_or(""),
                "move-to":queue.move_messages_to.as_deref().unwrap_or(""),
            });
            if let Some(dlq) = &queue.dead_letter_queue {
                data["dead-letters-queue"] =
                    serde_json::json!({"name":dlq.name,"max-receive-count":dlq.max_receive_count});
            }
            if let Some(seconds) = queue.message_retention_seconds {
                data["message-retention-period"] = serde_json::json!(seconds.saturating_mul(1000));
            }
            if let Some(bytes) = queue.maximum_message_size {
                data["maximum-message-size"] = serde_json::json!(bytes);
            }
            queues.insert(name, data);
        }
        Ok(Some(serde_json::to_vec_pretty(
            &serde_json::json!({"queues":queues}),
        )?))
    }
}

/// The stored fields of a message that change after it is sent.
struct MessageState {
    next_delivery: i64,
    receive_count: u32,
    first_receive: Option<i64>,
    receipt_handle: Option<String>,
}

/// Outcome of a group commit; the error text is the database's.
pub type CommitResult = Result<(), String>;

struct Job {
    events: Vec<QueueEvent>,
    definitions: Option<Vec<u8>>,
    done: oneshot::Sender<CommitResult>,
}

/// Commits engine events on one thread, in submission order, grouping whatever
/// is waiting into one transaction and one `fsync`. Callers submit while holding
/// the manager, then release it and wait, so a slow disk never blocks the engine
/// and no response is sent before its changes are durable.
///
/// A failed group answers all its requests with the error. Its events stay first
/// in line and are retried, because the engine has already applied them.
#[derive(Clone)]
pub struct Writer {
    persistence: Arc<Persistence>,
    sender: mpsc::Sender<Job>,
    /// Submitted jobs whose events are not yet durable.
    unsettled: Arc<AtomicUsize>,
    errors: Arc<AtomicU64>,
}

impl Writer {
    pub fn spawn(persistence: Persistence) -> Self {
        let persistence = Arc::new(persistence);
        let (sender, receiver) = mpsc::channel();
        let unsettled = Arc::new(AtomicUsize::new(0));
        let errors = Arc::new(AtomicU64::new(0));
        let loop_state = WriterLoop {
            persistence: persistence.clone(),
            unsettled: unsettled.clone(),
            errors: errors.clone(),
            failing: None,
        };
        std::thread::Builder::new()
            .name("rusticmq-writer".into())
            .spawn(move || loop_state.run(receiver))
            .expect("start the database writer thread");
        Self {
            persistence,
            sender,
            unsettled,
            errors,
        }
    }

    /// Queue events for commit. Call this while holding the manager so that
    /// commit order matches engine order. Without events, this still waits for
    /// anything already submitted: an answer can depend on earlier uncommitted
    /// state, such as a FIFO duplicate of a message that is not yet durable.
    /// Returns `None` when nothing is outstanding. On error, nothing was queued
    /// and the events are handed back.
    pub fn submit(
        &self,
        events: Vec<QueueEvent>,
        manager: &QueueManager,
    ) -> Result<Option<oneshot::Receiver<CommitResult>>, (Vec<QueueEvent>, PersistenceError)> {
        if events.is_empty() && self.unsettled.load(Ordering::Acquire) == 0 {
            return Ok(None);
        }
        let definitions = match self.persistence.definitions(&events, manager) {
            Ok(definitions) => definitions,
            Err(error) => return Err((events, error)),
        };
        let (done, result) = oneshot::channel();
        self.unsettled.fetch_add(1, Ordering::AcqRel);
        let job = Job {
            events,
            definitions,
            done,
        };
        match self.sender.send(job) {
            Ok(()) => Ok(Some(result)),
            Err(mpsc::SendError(job)) => {
                self.unsettled.fetch_sub(1, Ordering::AcqRel);
                Err((
                    job.events,
                    PersistenceError::Config("Database writer stopped".into()),
                ))
            }
        }
    }

    /// Failed commit attempts since the process started.
    pub fn errors(&self) -> u64 {
        self.errors.load(Ordering::Relaxed)
    }
}

/// A group being committed, including events retained from failed attempts.
#[derive(Default)]
struct Group {
    events: Vec<QueueEvent>,
    definitions: Option<Vec<u8>>,
    /// Jobs not yet durable, counted out of `Writer::unsettled` on success.
    jobs: usize,
    waiting: Vec<oneshot::Sender<CommitResult>>,
    body_bytes: usize,
}

impl Group {
    fn add(&mut self, job: Job) {
        self.body_bytes += job
            .events
            .iter()
            .map(|event| match event {
                // Updates store only the message's state, not its body.
                QueueEvent::MessageAdded { message, .. } => message.body.len(),
                _ => 0,
            })
            .sum::<usize>();
        self.events.extend(job.events);
        if job.definitions.is_some() {
            self.definitions = job.definitions;
        }
        self.jobs += 1;
        self.waiting.push(job.done);
    }

    fn is_full(&self) -> bool {
        self.events.len() >= MAX_BATCH_EVENTS || self.body_bytes >= MAX_BATCH_BODY_BYTES
    }
}

struct FailureLog {
    attempts: u64,
    logged_attempts: u64,
    logged_at: Instant,
}

struct WriterLoop {
    persistence: Arc<Persistence>,
    unsettled: Arc<AtomicUsize>,
    errors: Arc<AtomicU64>,
    failing: Option<FailureLog>,
}

impl WriterLoop {
    fn run(mut self, receiver: mpsc::Receiver<Job>) {
        let mut group = Group::default();
        loop {
            let open = if group.jobs == 0 {
                match receiver.recv() {
                    Ok(job) => {
                        group.add(job);
                        true
                    }
                    Err(mpsc::RecvError) => return,
                }
            } else {
                match receiver.recv_timeout(RETRY_INTERVAL) {
                    Ok(job) => {
                        group.add(job);
                        true
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => true,
                    Err(mpsc::RecvTimeoutError::Disconnected) => false,
                }
            };
            while !group.is_full() {
                let Ok(job) = receiver.try_recv() else { break };
                group.add(job);
            }
            self.commit(&mut group);
            if !open {
                return;
            }
        }
    }

    fn commit(&mut self, group: &mut Group) {
        match self
            .persistence
            .commit(&group.events, group.definitions.as_deref())
        {
            Ok(()) => {
                self.unsettled.fetch_sub(group.jobs, Ordering::AcqRel);
                let waiting = std::mem::take(&mut group.waiting);
                *group = Group::default();
                for done in waiting {
                    let _ = done.send(Ok(()));
                }
                if let Some(failure) = self.failing.take() {
                    eprintln!(
                        "Persistence recovered after {} failed commit attempts",
                        failure.attempts
                    );
                }
            }
            Err(error) => {
                self.errors.fetch_add(1, Ordering::Relaxed);
                self.log_failure(group, &error);
                let message = error.to_string();
                for done in group.waiting.drain(..) {
                    let _ = done.send(Err(message.clone()));
                }
            }
        }
    }

    /// A failing disk can fail thousands of requests per second, so at most one
    /// line per interval is logged. Event contents are never part of the line.
    fn log_failure(&mut self, group: &Group, error: &PersistenceError) {
        let now = Instant::now();
        let failure = self.failing.get_or_insert(FailureLog {
            attempts: 0,
            logged_attempts: 0,
            logged_at: now - FAILURE_LOG_INTERVAL,
        });
        failure.attempts += 1;
        if now.duration_since(failure.logged_at) >= FAILURE_LOG_INTERVAL {
            eprintln!(
                "Persistence commit failed ({} attempts since the last line), {} events kept for retry: {error}",
                failure.attempts - failure.logged_attempts,
                group.events.len()
            );
            failure.logged_attempts = failure.attempts;
            failure.logged_at = now;
        }
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
