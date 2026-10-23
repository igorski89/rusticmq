//! HOCON startup queue definitions, including upstream duration and FIFO syntax.
use crate::{CreateQueue, DeadLetterQueue, QueueConfig};
use hocon::{Hocon, HoconLoader};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

pub type ConfigError = Box<dyn std::error::Error + Send + Sync>;

pub struct AppConfig {
    pub bind: String,
    pub service: crate::sqs::ServerConfig,
    pub queues: Vec<QueueConfig>,
    pub queue_storage: Option<PathBuf>,
    pub message_storage: Option<PathBuf>,
    pub prune_messages: bool,
    pub enabled: bool,
    pub generate_node_address: bool,
}

impl AppConfig {
    pub fn load(path: Option<&Path>, now: i64) -> Result<Self, ConfigError> {
        let mut loader = HoconLoader::new().strict().load_str("queues {}\nrest-sqs {}\nnode-address {}\naws {}\nmessages-storage {}\nqueues-storage {}\nauto-create-queues {}")?;
        if let Some(path) = path {
            loader = loader.load_file(path)?;
        }
        let doc = loader.hocon()?;
        let rest = &doc["rest-sqs"];
        let bind_host = text(rest, "bind-hostname")?.unwrap_or_else(|| "0.0.0.0".into());
        let bind_port = port(rest, "bind-port", 9324)?;
        let node = &doc["node-address"];
        let host = text(node, "host")?.unwrap_or_else(|| "localhost".into());
        let protocol = text(node, "protocol")?.unwrap_or_else(|| "http".into());
        let node_port = port(node, "port", 9324)?;
        let context = text(node, "context-path")?.unwrap_or_default();
        let context = context.trim_matches('/');
        let aws = &doc["aws"];
        let sql = &doc["messages-storage"];
        let queues = &doc["queues-storage"];
        let relaxed_limits = match text(rest, "sqs-limits")?.as_deref() {
            None | Some("strict") => false,
            Some("relaxed") => true,
            Some(mode) => return Err(format!("Unknown sqs-limits mode: {mode}").into()),
        };
        let auto = &doc["auto-create-queues"];
        let auto_create_template = if boolean(auto, "enabled", false)? {
            let template = if absent(&auto["template"]) {
                Hocon::Hash(Default::default())
            } else {
                auto["template"].clone()
            };
            let queues = Hocon::Hash([("template".into(), template)].into_iter().collect());
            let document = Hocon::Hash([("queues".into(), queues)].into_iter().collect());
            Some(read_queues(&document, now)?.remove(0))
        } else {
            None
        };
        let message_storage = if boolean(sql, "enabled", false)? {
            let uri = text(sql, "uri")?.ok_or("messages-storage.uri is required")?;
            let path = uri
                .strip_prefix("jdbc:sqlite:")
                .or_else(|| uri.strip_prefix("sqlite:"))
                .ok_or("messages-storage.uri must use sqlite:PATH or jdbc:sqlite:PATH")?;
            Some(PathBuf::from(path))
        } else {
            Some(PathBuf::from(":memory:"))
        };
        Ok(Self {
            bind: format!("{bind_host}:{bind_port}"),
            service: crate::sqs::ServerConfig {
                base_url: format!(
                    "{protocol}://{host}:{node_port}{}",
                    if context.is_empty() {
                        String::new()
                    } else {
                        format!("/{context}")
                    }
                ),
                context_path: if context.is_empty() {
                    String::new()
                } else {
                    format!("/{context}")
                },
                region: text(aws, "region")?.unwrap_or_else(|| "elasticmq".into()),
                account_id: text(aws, "accountId")?.unwrap_or_else(|| "000000000000".into()),
                relaxed_limits,
                auto_create_template,
                wildcard_queue_urls: host == "*",
                max_content_length: size(
                    nested(
                        &doc,
                        &["pekko", "http", "server", "parsing", "max-content-length"],
                    ),
                    "max-content-length",
                    8 * 1024 * 1024,
                )?,
                config_file: path.map(|path| path.display().to_string()),
                configured_queues: 0,
                persistent_storage: false,
            },
            queues: read_queues(&doc, now)?,
            queue_storage: if boolean(queues, "enabled", false)? {
                Some(PathBuf::from(
                    text(queues, "path")?
                        .filter(|s| !s.is_empty())
                        .ok_or("queues-storage.path is required")?,
                ))
            } else {
                None
            },
            message_storage,
            prune_messages: boolean(sql, "prune-data-on-init", false)?,
            enabled: boolean(rest, "enabled", true)?,
            generate_node_address: boolean(&doc, "generate-node-address", false)?,
        })
    }
}

fn port(doc: &Hocon, key: &str, default: u16) -> Result<u16, ConfigError> {
    if absent(&doc[key]) {
        return Ok(default);
    }
    doc[key]
        .as_i64()
        .and_then(|port| u16::try_from(port).ok())
        .ok_or_else(|| format!("Invalid {key}").into())
}

pub fn read_queue_file(path: &Path, now: i64) -> Result<Vec<QueueConfig>, ConfigError> {
    read_queues(&HoconLoader::new().strict().load_file(path)?.hocon()?, now)
}

pub fn read_queues(document: &Hocon, now: i64) -> Result<Vec<QueueConfig>, ConfigError> {
    let Hocon::Hash(queues) = &document["queues"] else {
        if absent(&document["queues"]) {
            return Ok(Vec::new());
        }
        return Err("queues must be an object".into());
    };
    queues
        .iter()
        .map(|(name, value)| {
            let fifo = boolean(value, "fifo", false)?;
            let name = if fifo && !name.ends_with(".fifo") {
                format!("{name}.fifo")
            } else {
                name.clone()
            };
            let mut request = CreateQueue::named(name);
            request.fifo = fifo;
            request.content_based_deduplication =
                boolean(value, "contentBasedDeduplication", false)?;
            request.visibility_timeout_ms = duration(value, "defaultVisibilityTimeout")?;
            request.delay_ms = duration(value, "delay")?;
            request.receive_wait_ms = duration(value, "receiveMessageWait")?;
            request.copy_messages_to = text(value, "copyTo")?.filter(|s| !s.is_empty());
            request.move_messages_to = text(value, "moveTo")?.filter(|s| !s.is_empty());
            if !absent(&value["deadLettersQueue"]) {
                let dead_letters = &value["deadLettersQueue"];
                let name =
                    text(dead_letters, "name")?.ok_or("deadLettersQueue.name is required")?;
                let count = dead_letters["maxReceiveCount"]
                    .as_i64()
                    .filter(|count| (1..=1000).contains(count))
                    .ok_or("Invalid maxReceiveCount")?;
                request.dead_letter_queue = Some(DeadLetterQueue {
                    name,
                    max_receive_count: count as u32,
                });
            }
            request.tags = match &value["tags"] {
                Hocon::Hash(tags) => tags
                    .iter()
                    .map(|(key, value)| {
                        value
                            .as_string()
                            .map(|value| (key.clone(), value))
                            .ok_or_else(|| ConfigError::from("Tag values must be strings"))
                    })
                    .collect::<Result<BTreeMap<_, _>, _>>()?,
                value if absent(value) => BTreeMap::new(),
                _ => return Err("tags must be an object".into()),
            };
            let created = if absent(&value["created"]) {
                now
            } else {
                value["created"]
                    .as_i64()
                    .ok_or("Invalid created timestamp")?
            };
            let mut queue = request.into_config(created);
            queue.last_modified = if absent(&value["lastModified"]) {
                now
            } else {
                value["lastModified"]
                    .as_i64()
                    .ok_or("Invalid lastModified timestamp")?
            };
            Ok(queue)
        })
        .collect()
}

pub(crate) fn absent(value: &Hocon) -> bool {
    matches!(
        value,
        Hocon::Null | Hocon::BadValue(hocon::Error::MissingKey)
    )
}
/// Follows nested keys, treating a missing or non-object parent as absent.
fn nested<'a>(document: &'a Hocon, keys: &[&str]) -> &'a Hocon {
    static MISSING: Hocon = Hocon::Null;
    keys.iter()
        .try_fold(document, |value, key| match value {
            Hocon::Hash(hash) => hash.get(*key),
            _ => None,
        })
        .unwrap_or(&MISSING)
}
/// Reads a HOCON size such as `500m`; `infinite` yields `None`.
fn size(value: &Hocon, key: &str, default: usize) -> Result<Option<usize>, ConfigError> {
    if absent(value) {
        return Ok(Some(default));
    }
    if value.as_string().as_deref() == Some("infinite") {
        return Ok(None);
    }
    match value {
        Hocon::Integer(bytes) if *bytes < 0 => None,
        _ => value
            .as_bytes()
            .and_then(|bytes| usize::try_from(bytes).ok()),
    }
    .map(Some)
    .ok_or_else(|| format!("Invalid size for {key}").into())
}
pub(crate) fn text(document: &Hocon, key: &str) -> Result<Option<String>, ConfigError> {
    let value = &document[key];
    if absent(value) {
        return Ok(None);
    }
    value
        .as_string()
        .map(Some)
        .ok_or_else(|| format!("{key} must be a string").into())
}
pub(crate) fn boolean(document: &Hocon, key: &str, default: bool) -> Result<bool, ConfigError> {
    let value = &document[key];
    if absent(value) {
        return Ok(default);
    }
    value
        .as_bool()
        .ok_or_else(|| format!("{key} must be a boolean").into())
}
fn duration(document: &Hocon, key: &str) -> Result<Option<u64>, ConfigError> {
    let value = &document[key];
    if absent(value) {
        return Ok(None);
    }
    let millis = value
        .as_milliseconds()
        .filter(|value| value.is_finite() && *value >= 0.0 && *value < u64::MAX as f64)
        .ok_or_else(|| format!("Invalid duration for {key}"))?;
    Ok(Some(millis as u64))
}
