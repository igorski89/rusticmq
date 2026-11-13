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
        let origin = path.map_or_else(
            || "the configuration".into(),
            |path| path.display().to_string(),
        );
        let rest = &doc["rest-sqs"];
        let bind_host = text(rest, "bind-hostname")?.unwrap_or_else(|| "0.0.0.0".into());
        let bind_port = port(rest, "bind-port", 9324)?;
        let node = &doc["node-address"];
        // Since 2.0, queue URLs follow each request's host unless a host is set.
        let host = text(node, "host")?.unwrap_or_else(|| "*".into());
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
            Some(read_queues(&document, now, &origin, "auto-create-queues")?.remove(0))
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
            // Messages stay in the engine only, with no database at all.
            None
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
                account_id: text(aws, renamed(aws, "account-id", "accountId", "aws", &origin))?
                    .unwrap_or_else(|| "000000000000".into()),
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
                storage: crate::sqs::Storage::Memory,
            },
            queues: read_queues(&doc, now, &origin, "queues")?,
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
    read_queues(
        &HoconLoader::new().strict().load_file(path)?.hocon()?,
        now,
        &path.display().to_string(),
        "queues",
    )
}

/// The key to read: a 1.x camelCase key if present, with a deprecation warning,
/// otherwise its 2.0 kebab-case replacement. As upstream, the old key wins.
fn renamed<'k>(
    document: &Hocon,
    key: &'k str,
    deprecated: &'k str,
    section: &str,
    origin: &str,
) -> &'k str {
    if absent(&document[deprecated]) {
        return key;
    }
    eprintln!(
        "Configuration key '{section}.{deprecated}' ({origin}) is deprecated and will be removed in a future release, use '{key}' instead"
    );
    deprecated
}

/// Reads queue definitions from `document["queues"]`. `origin` and `section`
/// name them in deprecation warnings.
pub fn read_queues(
    document: &Hocon,
    now: i64,
    origin: &str,
    section: &str,
) -> Result<Vec<QueueConfig>, ConfigError> {
    let Hocon::Hash(queues) = &document["queues"] else {
        if absent(&document["queues"]) {
            return Ok(Vec::new());
        }
        return Err("queues must be an object".into());
    };
    queues
        .iter()
        .map(|(name, value)| {
            let section = if section == "auto-create-queues" {
                "auto-create-queues.template".to_owned()
            } else {
                format!("{section}.{name}")
            };
            let key =
                |new: &'static str, old: &'static str| renamed(value, new, old, &section, origin);
            let fifo = boolean(value, "fifo", false)?;
            let name = if fifo && !name.ends_with(".fifo") {
                format!("{name}.fifo")
            } else {
                name.clone()
            };
            let mut request = CreateQueue::named(name);
            request.fifo = fifo;
            request.content_based_deduplication = boolean(
                value,
                key("content-based-deduplication", "contentBasedDeduplication"),
                false,
            )?;
            request.visibility_timeout_ms = duration(
                value,
                key("default-visibility-timeout", "defaultVisibilityTimeout"),
            )?;
            request.delay_ms = duration(value, "delay")?;
            request.receive_wait_ms =
                duration(value, key("receive-message-wait", "receiveMessageWait"))?;
            request.copy_messages_to =
                text(value, key("copy-to", "copyTo"))?.filter(|s| !s.is_empty());
            request.move_messages_to =
                text(value, key("move-to", "moveTo"))?.filter(|s| !s.is_empty());
            let dead_letters_key = key("dead-letters-queue", "deadLettersQueue");
            if !absent(&value[dead_letters_key]) {
                let dead_letters = &value[dead_letters_key];
                let name = text(dead_letters, "name")?
                    .ok_or_else(|| format!("{dead_letters_key}.name is required"))?;
                let section = format!("{section}.{dead_letters_key}");
                let count_key = renamed(
                    dead_letters,
                    "max-receive-count",
                    "maxReceiveCount",
                    &section,
                    origin,
                );
                let count = dead_letters[count_key]
                    .as_i64()
                    .filter(|count| (1..=1000).contains(count))
                    .ok_or_else(|| format!("Invalid {dead_letters_key}.{count_key}"))?;
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
            request.message_retention_seconds = duration(value, "message-retention-period")?
                .map(|millis| match millis / 1000 {
                    0 => Err(ConfigError::from("Invalid message-retention-period")),
                    seconds => Ok(seconds),
                })
                .transpose()?;
            request.maximum_message_size = match &value["maximum-message-size"] {
                size if absent(size) => None,
                size => Some(
                    size.as_bytes()
                        .and_then(|bytes| u32::try_from(bytes).ok())
                        .filter(|bytes| (1..=i32::MAX as u32).contains(bytes))
                        .ok_or("Invalid maximum-message-size")?,
                ),
            };
            let created = if absent(&value["created"]) {
                now
            } else {
                value["created"]
                    .as_i64()
                    .ok_or("Invalid created timestamp")?
            };
            let mut queue = request.into_config(created);
            let last_modified = key("last-modified", "lastModified");
            queue.last_modified = if absent(&value[last_modified]) {
                now
            } else {
                value[last_modified]
                    .as_i64()
                    .ok_or_else(|| format!("Invalid {last_modified} timestamp"))?
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
