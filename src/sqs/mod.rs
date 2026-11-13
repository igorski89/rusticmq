//! SQS wire protocols and HTTP service. Authentication is deliberately ignored,
//! matching ElasticMQ's local development service.
pub mod digest;
mod protocol;

use crate::*;
use axum::{
    Router,
    body::Bytes,
    extract::{
        DefaultBodyLimit, FromRequest, OriginalUri, Request, State, rejection::BytesRejection,
    },
    http::{
        HeaderMap, HeaderValue, Method, StatusCode, Uri,
        header::{
            ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS,
            ACCESS_CONTROL_ALLOW_ORIGIN, CONTENT_TYPE,
        },
    },
    response::{IntoResponse, Response},
    routing::get,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use bigdecimal::BigDecimal;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, oneshot, watch};

type ApiResult<T> = Result<T, ApiError>;

/// Receives returning at least this many body bytes yield before building
/// their response; see `receive`.
const YIELD_BEFORE_RESPONSE_BYTES: usize = 64 * 1024;

/// A request's commit. Await it after releasing the manager, and before
/// answering: nothing is acknowledged until its changes are durable.
#[must_use]
enum Commit {
    Settled(ApiResult<()>),
    Pending(oneshot::Receiver<crate::persistence::CommitResult>),
}

impl Commit {
    async fn wait(self) -> ApiResult<()> {
        match self {
            Self::Settled(result) => result,
            Self::Pending(result) => match result.await {
                Ok(result) => result.map_err(|error| ApiError::new("InternalError", error)),
                Err(_) => Err(ApiError::new("InternalError", "Database writer stopped")),
            },
        }
    }
}

#[derive(Debug)]
struct ApiError {
    code: String,
    message: String,
}

impl ApiError {
    fn json_type(&self) -> String {
        let name = match self.code.as_str() {
            "AWS.SimpleQueueService.NonExistentQueue" => "QueueDoesNotExist",
            "QueueAlreadyExists" => "QueueNameExists",
            "AWS.SimpleQueueService.UnsupportedOperation" => "UnsupportedOperation",
            name => name.strip_prefix("AWS.SimpleQueueService.").unwrap_or(name),
        };
        if name == "MissingParameter" {
            name.into()
        } else {
            format!("com.amazonaws.sqs#{name}")
        }
    }
    fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
    fn invalid(message: impl Into<String>) -> Self {
        Self::new("InvalidParameterValue", message)
    }
}

impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        if matches!(error, Error::QueueDoesNotExist(_)) {
            return Self::new(
                "AWS.SimpleQueueService.NonExistentQueue",
                "The specified queue does not exist.",
            );
        }
        let code = match &error {
            Error::QueueDoesNotExist(_) => "AWS.SimpleQueueService.NonExistentQueue",
            Error::QueueAlreadyExists(_) => "QueueAlreadyExists",
            Error::InvalidReceiptHandle(_) => "ReceiptHandleIsInvalid",
            Error::InvalidMoveTaskHandle(_) => "ResourceNotFoundException",
            Error::MoveTaskAlreadyRunning(_) => "AWS.SimpleQueueService.UnsupportedOperation",
            _ => "InvalidParameterValue",
        };
        Self::new(code, error.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub base_url: String,
    pub region: String,
    pub account_id: String,
    pub context_path: String,
    pub relaxed_limits: bool,
    pub auto_create_template: Option<QueueConfig>,
    pub wildcard_queue_urls: bool,
    /// Request body limit in bytes; `None` disables it (pekko's `infinite`).
    pub max_content_length: Option<usize>,
    /// Startup details reported by `rusticmq_config_info` and `/metrics` gauges.
    pub config_file: Option<String>,
    pub configured_queues: usize,
    pub storage: Storage,
}

/// Where messages are kept, reported by `rusticmq_config_info`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Storage {
    /// In the engine only, with no database, like ElasticMQ's default.
    #[default]
    Memory,
    /// An in-memory SQLite database (`:memory:`): the durable write path
    /// without disk I/O.
    SqliteMemory,
    Sqlite,
}

impl Storage {
    pub fn label(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::SqliteMemory => "sqlite-memory",
            Self::Sqlite => "sqlite",
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            base_url: "http://localhost:9324".into(),
            region: "elasticmq".into(),
            account_id: "000000000000".into(),
            context_path: String::new(),
            relaxed_limits: false,
            auto_create_template: None,
            wildcard_queue_urls: false,
            max_content_length: Some(8 * 1024 * 1024),
            config_file: None,
            configured_queues: 0,
            storage: Storage::Memory,
        }
    }
}

impl ServerConfig {
    fn queue_url(&self, base_url: &str, name: &str) -> String {
        format!(
            "{}/{}/{name}",
            base_url.trim_end_matches('/'),
            if self.account_id.is_empty() {
                "queue"
            } else {
                &self.account_id
            }
        )
    }
    fn arn(&self, name: &str) -> String {
        format!("arn:aws:sqs:{}:{}:{name}", self.region, self.account_id)
    }
}

#[derive(Clone)]
pub struct SqsService {
    pub manager: Arc<Mutex<QueueManager>>,
    /// Shared: axum clones the service for every request.
    pub config: Arc<ServerConfig>,
    changed: watch::Sender<u64>,
    stopping: watch::Sender<bool>,
    writer: Option<crate::persistence::Writer>,
    too_large: Arc<TooLarge>,
    started: Timestamp,
    /// This request's base for queue URLs, from its `Host` header, when queue
    /// URLs follow the request (`node-address.host = "*"`).
    request_base_url: Option<Arc<str>>,
}

/// Request bodies rejected for exceeding `max-content-length`. The action
/// comes from a client header, so labels are limited to the send actions.
#[derive(Default)]
struct TooLarge([std::sync::atomic::AtomicU64; 3]);

impl TooLarge {
    const ACTIONS: [&str; 3] = ["SendMessage", "SendMessageBatch", "other"];

    fn record(&self, action: &str) {
        let index = Self::ACTIONS[..2]
            .iter()
            .position(|known| *known == action)
            .unwrap_or(2);
        self.0[index].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn counts(&self) -> impl Iterator<Item = (&'static str, u64)> + '_ {
        Self::ACTIONS
            .into_iter()
            .zip(&self.0)
            .map(|(action, count)| (action, count.load(std::sync::atomic::Ordering::Relaxed)))
    }
}

impl SqsService {
    pub fn new(mut manager: QueueManager, config: ServerConfig) -> Self {
        manager.set_relaxed_limits(config.relaxed_limits);
        let (changed, _) = watch::channel(0);
        let (stopping, _) = watch::channel(false);
        Self {
            manager: Arc::new(Mutex::new(manager)),
            config: Arc::new(config),
            changed,
            stopping,
            writer: None,
            too_large: Arc::default(),
            started: now(),
            request_base_url: None,
        }
    }

    fn queue_url(&self, name: &str) -> String {
        let base = self
            .request_base_url
            .as_deref()
            .unwrap_or(&self.config.base_url);
        self.config.queue_url(base, name)
    }

    pub fn router(self) -> Router {
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(self.clone().expire_periodically());
        }
        let router = Router::new()
            .route("/health", get(|| async { "OK" }))
            .route("/metrics", get(metrics));
        let router = if self.config.context_path.is_empty() {
            router.fallback(handle)
        } else {
            router.nest(&self.config.context_path, Router::new().fallback(handle))
        };
        let limit = match self.config.max_content_length {
            Some(bytes) => DefaultBodyLimit::max(bytes),
            None => DefaultBodyLimit::disable(),
        };
        router.layer(limit).with_state(self)
    }

    /// Commit changes through `persistence`. Without a database or a queue
    /// definitions file there is nothing to write, so no writer is started
    /// and requests are answered without waiting on one.
    pub fn with_persistence(mut self, persistence: crate::persistence::Persistence) -> Self {
        if persistence.writes_anything() {
            self.writer = Some(crate::persistence::Writer::spawn(persistence));
        }
        self
    }

    /// Commit anything still outstanding, for use after the HTTP server stops.
    pub async fn flush(&self) -> Result<(), String> {
        let commit = {
            let mut manager = self.manager.lock().await;
            self.notify_change(&mut manager)
        };
        commit.wait().await.map_err(|error| error.message)
    }

    /// Delete expired messages every second, as upstream does, until shutdown.
    /// A queue whose messages can't expire costs one comparison per sweep.
    async fn expire_periodically(self) {
        let mut stopping = self.stopping.subscribe();
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = interval.tick() => {}
                _ = stopping.wait_for(|stopping| *stopping) => break,
            }
            let commit = {
                let mut manager = self.manager.lock().await;
                if manager.expire_messages(now()) == 0 {
                    continue;
                }
                self.notify_change(&mut manager)
            };
            if let Err(error) = commit.wait().await {
                eprintln!("Message expiry persistence: {}", error.message);
            }
        }
    }

    /// Finish outstanding long polls before the HTTP server drains connections.
    pub fn shutdown(&self) {
        self.stopping.send_replace(true);
    }

    /// The largest message the queue accepts, in bytes; `None` for any size.
    fn maximum_message_size(&self, queue: &QueueConfig) -> Option<u32> {
        queue
            .maximum_message_size
            .or((!self.config.relaxed_limits).then_some(STRICT_MAXIMUM_MESSAGE_SIZE))
    }

    /// `MessageRetentionPeriod` in seconds: 60 seconds to 14 days under strict
    /// limits, any positive value under relaxed limits.
    fn retention_attribute(&self, attributes: &BTreeMap<String, String>) -> ApiResult<Option<u64>> {
        let range = (!self.config.relaxed_limits).then_some(60..=14 * 24 * 60 * 60);
        positive_attribute(attributes, "MessageRetentionPeriod", range, u64::MAX)
    }

    /// `MaximumMessageSize` in bytes: 1 KiB to 1 MiB under strict limits, any
    /// positive 32-bit value under relaxed limits.
    fn maximum_size_attribute(
        &self,
        attributes: &BTreeMap<String, String>,
    ) -> ApiResult<Option<u32>> {
        let range =
            (!self.config.relaxed_limits).then_some(1024..=u64::from(STRICT_MAXIMUM_MESSAGE_SIZE));
        positive_attribute(attributes, "MaximumMessageSize", range, i32::MAX as u64)
            .map(|bytes| bytes.map(|bytes| bytes as u32))
    }

    fn wait_limit(&self) -> u64 {
        if self.config.relaxed_limits {
            i64::MAX as u64 / 1000
        } else {
            20
        }
    }

    fn ensure_queue(
        &self,
        manager: &mut QueueManager,
        name: &str,
        now: Timestamp,
    ) -> ApiResult<()> {
        if manager.queue(name).is_ok() {
            return Ok(());
        }
        if let Some(template) = &self.config.auto_create_template {
            manager.create_queue(
                CreateQueue {
                    name: name.into(),
                    fifo: name.ends_with(".fifo") || template.fifo,
                    content_based_deduplication: template.content_based_deduplication,
                    visibility_timeout_ms: Some(template.visibility_timeout_ms),
                    delay_ms: Some(template.delay_ms),
                    receive_wait_ms: Some(template.receive_wait_ms),
                    dead_letter_queue: template.dead_letter_queue.clone(),
                    copy_messages_to: template.copy_messages_to.clone(),
                    move_messages_to: template.move_messages_to.clone(),
                    tags: template.tags.clone(),
                    message_retention_seconds: template.message_retention_seconds,
                    maximum_message_size: template.maximum_message_size,
                },
                now,
            )?;
        } else {
            manager.queue(name)?;
        }
        Ok(())
    }

    async fn execute(
        &self,
        action: &str,
        request: Value,
        tracing: Option<String>,
    ) -> ApiResult<Value> {
        if action == "ReceiveMessage" {
            return self.receive(&request).await;
        }
        if action == "SendMessage" || action == "SendMessageBatch" {
            return self.send(action, request, tracing).await;
        }
        let (result, commit) = {
            let mut manager = self.manager.lock().await;
            let result = self.dispatch(&mut manager, action, &request, tracing, now());
            let commit = self.notify_change(&mut manager);
            (result, commit)
        };
        // Start the worker before waiting: a client disconnecting during the
        // wait cancels this request, but the task has already been accepted.
        if action == "StartMessageMoveTask"
            && let Ok(value) = &result
            && let Some(handle) = value["TaskHandle"].as_str()
        {
            let service = self.clone();
            let handle = handle.to_owned();
            tokio::spawn(async move {
                service.run_move_task(handle).await;
            });
        }
        commit.wait().await?;
        result
    }

    /// Hand the engine's events to the database writer, while holding the
    /// manager so that commit order matches engine order.
    fn notify_change(&self, manager: &mut QueueManager) -> Commit {
        let changed = manager.take_changed();
        let events = manager.drain_events();
        let commit = match &self.writer {
            None => Commit::Settled(Ok(())),
            Some(writer) => match writer.submit(events, manager) {
                Ok(Some(result)) => Commit::Pending(result),
                Ok(None) => Commit::Settled(Ok(())),
                Err((events, error)) => {
                    eprintln!("Persistence submit failed: {error}");
                    manager.requeue_events(events);
                    return Commit::Settled(Err(ApiError::new("InternalError", error.to_string())));
                }
            },
        };
        if changed {
            self.changed
                .send_modify(|version| *version = version.wrapping_add(1));
        }
        commit
    }

    async fn run_move_task(&self, handle: String) {
        loop {
            // Earlier changes must be durable before more messages move.
            let pending = {
                let mut manager = self.manager.lock().await;
                self.notify_change(&mut manager)
            };
            if let Err(error) = pending.wait().await {
                eprintln!("Message move task {handle} persistence: {}", error.message);
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }
            let (step, commit) = {
                let mut manager = self.manager.lock().await;
                let step = manager.step_move_task(&handle, now());
                let commit = self.notify_change(&mut manager);
                (step, commit)
            };
            if let Err(error) = commit.wait().await {
                eprintln!("Message move task {handle} persistence: {}", error.message);
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }
            match step {
                Ok(Some(delay)) if delay.is_zero() => tokio::task::yield_now().await,
                Ok(Some(delay)) => tokio::time::sleep(delay).await,
                Ok(None) => break,
                Err(error) => {
                    eprintln!("Message move task {handle}: {error}");
                    break;
                }
            }
        }
    }

    /// Send without holding the engine lock for parsing, validation, digests
    /// or the response; only queue lookups and the engine mutation run under it.
    async fn send(
        &self,
        action: &str,
        request: Value,
        tracing: Option<String>,
    ) -> ApiResult<Value> {
        let (result, commit) = if action == "SendMessage" {
            let name = queue_name(&request, &self.config.context_path);
            let prepared = name
                .is_ok()
                .then(|| prepare_send(&request, tracing, self.config.relaxed_limits));
            let mut manager = self.manager.lock().await;
            let now = now();
            let result = name.and_then(|name| {
                self.ensure_queue(&mut manager, &name, now)?;
                let prepared = prepared.expect("prepared with the queue name");
                self.send_prepared(&mut manager, &name, prepared, now)
            });
            (
                result.map(Sent::into_json),
                self.notify_change(&mut manager),
            )
        } else {
            let prepared = self.prepare_send_batch(&request, tracing);
            let mut manager = self.manager.lock().await;
            let now = now();
            let result = match &prepared.name {
                Ok(name) => self.ensure_queue(&mut manager, name, now),
                Err(_) => Ok(()),
            }
            .and_then(|()| self.send_batch_prepared(&mut manager, prepared, now));
            (
                result.map(SentBatch::into_json),
                self.notify_change(&mut manager),
            )
        };
        commit.wait().await?;
        result
    }

    fn prepare_send_batch(&self, p: &Value, tracing: Option<String>) -> PreparedBatch {
        let relaxed = self.config.relaxed_limits;
        let name = queue_name(p, &self.config.context_path);
        let entries = batch_entries(p, relaxed).map(|entries| {
            entries
                .iter()
                .map(|entry| {
                    (
                        entry["Id"].clone(),
                        prepare_send(entry, tracing.clone(), relaxed),
                    )
                })
                .collect::<Vec<(Value, PreparedSend)>>()
        });
        // As upstream, the whole batch is held to the SQS message size limit,
        // whatever the queue's own MaximumMessageSize.
        let entries = entries.and_then(|entries| {
            let total: u64 = entries.iter().map(|(_, entry)| entry.size).sum();
            if !relaxed && total > u64::from(STRICT_MAXIMUM_MESSAGE_SIZE) {
                return Err(ApiError::new(
                    "AWS.SimpleQueueService.BatchRequestTooLong",
                    "Batch requests cannot be longer than the maximum allowed message size.",
                ));
            }
            Ok(entries)
        });
        PreparedBatch { name, entries }
    }

    fn send_batch_prepared(
        &self,
        manager: &mut QueueManager,
        prepared: PreparedBatch,
        now: Timestamp,
    ) -> ApiResult<SentBatch> {
        let name = prepared.name?;
        manager.queue(&name)?;
        let entries = prepared.entries?;
        Ok(SentBatch(
            entries
                .into_iter()
                .map(|(id, entry)| {
                    let result = self.send_prepared(manager, &name, entry, now);
                    (id, result)
                })
                .collect(),
        ))
    }

    fn send_prepared(
        &self,
        manager: &mut QueueManager,
        name: &str,
        prepared: PreparedSend,
        now: Timestamp,
    ) -> ApiResult<Sent> {
        let queue = manager.queue(name)?;
        let fifo = queue.fifo;
        let maximum_size = self.maximum_message_size(queue);
        let (body, attributes) = prepared.content?;
        if let Some(maximum) = maximum_size
            && prepared.size > u64::from(maximum)
        {
            return Err(ApiError::invalid(format!(
                "One or more parameters are invalid. Reason: Message must be shorter than {maximum} bytes."
            )));
        }
        let (group_id, deduplication_id) = prepared.ids?;
        if fifo && group_id.is_none() {
            return Err(ApiError::new(
                "MissingParameter",
                "MessageGroupId is required",
            ));
        }
        if !fifo && deduplication_id.is_some() {
            return Err(ApiError::invalid(
                "MessageDeduplicationId is only valid for FIFO queues",
            ));
        }
        let delay = prepared.delay?;
        if fifo && delay.is_some_and(|seconds| seconds > 0) {
            return Err(ApiError::invalid(
                "The request includes parameter DelaySeconds that is not valid for this queue type",
            ));
        }
        let tracing_id = prepared.tracing?;
        let message = manager.send_message(
            name,
            SendMessage {
                body,
                attributes,
                group_id,
                deduplication_id,
                delay_ms: delay.filter(|_| !fifo).map(|seconds| seconds * 1000),
                tracing_id,
                ..Default::default()
            },
            now,
        )?;
        Ok(Sent {
            id: message.id,
            sequence_number: message.sequence_number,
            body_digest: prepared.body_digest,
            attribute_digest: prepared.attribute_digest,
        })
    }

    fn dispatch(
        &self,
        manager: &mut QueueManager,
        action: &str,
        p: &Value,
        tracing: Option<String>,
        now: Timestamp,
    ) -> ApiResult<Value> {
        if action == "GetQueueUrl" {
            self.ensure_queue(manager, required(p, "QueueName")?, now)?;
        } else if matches!(
            action,
            "SendMessage"
                | "SendMessageBatch"
                | "ReceiveMessage"
                | "DeleteQueue"
                | "PurgeQueue"
                | "DeleteMessage"
                | "DeleteMessageBatch"
                | "ChangeMessageVisibility"
                | "ChangeMessageVisibilityBatch"
                | "GetQueueAttributes"
                | "SetQueueAttributes"
                | "TagQueue"
                | "UntagQueue"
                | "ListQueueTags"
                | "ListDeadLetterSourceQueues"
        ) {
            self.ensure_queue(manager, &queue_name(p, &self.config.context_path)?, now)?;
        }
        match action {
            "CreateQueue" => {
                let attributes = string_map(p.get("Attributes"))?;
                let mut request = CreateQueue::named(required(p, "QueueName")?);
                request.fifo = boolean(&attributes, "FifoQueue")?.unwrap_or(false);
                validate_queue_name_wire(&request.name, request.fifo, self.config.relaxed_limits)?;
                request.content_based_deduplication =
                    boolean(&attributes, "ContentBasedDeduplication")?.unwrap_or(false);
                request.visibility_timeout_ms =
                    seconds_attribute(&attributes, "VisibilityTimeout", 43_200)?;
                request.delay_ms = seconds_attribute(&attributes, "DelaySeconds", 900)?;
                request.receive_wait_ms = seconds_attribute(
                    &attributes,
                    "ReceiveMessageWaitTimeSeconds",
                    self.wait_limit(),
                )?;
                request.dead_letter_queue = redrive(attributes.get("RedrivePolicy"), manager)?;
                request.message_retention_seconds = self.retention_attribute(&attributes)?;
                request.maximum_message_size = self.maximum_size_attribute(&attributes)?;
                request.tags = string_map(p.get("tags").or_else(|| p.get("Tags")))?;
                for key in attributes.keys() {
                    validate_queue_attribute(key, true)?;
                }
                let queue = manager.create_queue(request, now)?;
                Ok(json!({"QueueUrl": self.queue_url(&queue.name)}))
            }
            "ListQueues" => Ok(
                json!({"QueueUrls": manager.list_queues(optional(p, "QueueNamePrefix")?).iter().map(|name| self.queue_url(name)).collect::<Vec<_>>()}),
            ),
            "GetQueueUrl" => {
                let name = required(p, "QueueName")?;
                manager.queue(name)?;
                Ok(json!({"QueueUrl": self.queue_url(name)}))
            }
            "AddPermission" | "RemovePermission" => Ok(json!({})),
            "SendMessageBatch" => {
                let prepared = self.prepare_send_batch(p, tracing);
                self.send_batch_prepared(manager, prepared, now)
                    .map(SentBatch::into_json)
            }
            "DeleteMessageBatch" | "ChangeMessageVisibilityBatch" => {
                let name = queue_name(p, &self.config.context_path)?;
                manager.queue(&name)?;
                let entries = batch_entries(p, self.config.relaxed_limits)?;
                let mut successful = Vec::new();
                let mut failed = Vec::new();
                for entry in entries {
                    let mut entry = entry.clone();
                    entry["QueueUrl"] = p["QueueUrl"].clone();
                    let single = action.strip_suffix("Batch").unwrap();
                    match self.dispatch(manager, single, &entry, tracing.clone(), now) {
                        Ok(mut result) => { result["Id"] = entry["Id"].clone(); successful.push(result); }
                        Err(error) => failed.push(json!({"Id":entry["Id"], "Code":error.code, "Message":error.message, "SenderFault":true})),
                    }
                }
                Ok(json!({"Successful":successful, "Failed":failed}))
            }
            "DeleteQueue" => {
                let name = queue_name(p, &self.config.context_path)?;
                manager.queue(&name)?;
                manager.delete_queue(&name);
                Ok(json!({}))
            }
            "PurgeQueue" => {
                manager.purge_queue(&queue_name(p, &self.config.context_path)?)?;
                Ok(json!({}))
            }
            "DeleteMessage" => {
                manager.delete_message(
                    &queue_name(p, &self.config.context_path)?,
                    required(p, "ReceiptHandle")?,
                )?;
                Ok(json!({}))
            }
            "ChangeMessageVisibility" => {
                let timeout = integer(p, "VisibilityTimeout", None, 43_200)?;
                manager.change_visibility(
                    &queue_name(p, &self.config.context_path)?,
                    required(p, "ReceiptHandle")?,
                    timeout * 1000,
                    now,
                )?;
                Ok(json!({}))
            }
            "SendMessage" => {
                let name = queue_name(p, &self.config.context_path)?;
                let prepared = prepare_send(p, tracing, self.config.relaxed_limits);
                self.send_prepared(manager, &name, prepared, now)
                    .map(Sent::into_json)
            }
            "GetQueueAttributes" => {
                let name = queue_name(p, &self.config.context_path)?;
                let config = manager.queue(&name)?;
                let stats = manager.statistics(&name, now)?;
                let mut values = BTreeMap::from([
                    (
                        "VisibilityTimeout",
                        (config.visibility_timeout_ms / 1000).to_string(),
                    ),
                    ("DelaySeconds", (config.delay_ms / 1000).to_string()),
                    (
                        "ReceiveMessageWaitTimeSeconds",
                        (config.receive_wait_ms / 1000).to_string(),
                    ),
                    ("CreatedTimestamp", (config.created / 1000).to_string()),
                    (
                        "LastModifiedTimestamp",
                        (config.last_modified / 1000).to_string(),
                    ),
                    ("QueueArn", self.config.arn(&name)),
                    ("ApproximateNumberOfMessages", stats.visible.to_string()),
                    (
                        "ApproximateNumberOfMessagesNotVisible",
                        stats.invisible.to_string(),
                    ),
                    (
                        "ApproximateNumberOfMessagesDelayed",
                        stats.delayed.to_string(),
                    ),
                ]);
                // Under relaxed limits, an attribute the queue doesn't set is omitted.
                if let Some(seconds) = manager.message_retention_seconds(config) {
                    values.insert("MessageRetentionPeriod", seconds.to_string());
                }
                if let Some(bytes) = self.maximum_message_size(config) {
                    values.insert("MaximumMessageSize", bytes.to_string());
                }
                if config.fifo {
                    values.insert("FifoQueue", "true".into());
                    values.insert(
                        "ContentBasedDeduplication",
                        config.content_based_deduplication.to_string(),
                    );
                }
                if let Some(dlq) = &config.dead_letter_queue {
                    values.insert("RedrivePolicy", json!({"deadLetterTargetArn":self.config.arn(&dlq.name), "maxReceiveCount":dlq.max_receive_count}).to_string());
                }
                let names = strings(p, "AttributeNames")?;
                for name in &names {
                    if name != "All" && !values.contains_key(name.as_str()) {
                        validate_queue_attribute(name, true)?;
                    }
                }
                if !names.iter().any(|n| n == "All") {
                    values.retain(|key, _| names.iter().any(|n| n == key));
                }
                Ok(json!({"Attributes":values}))
            }
            "SetQueueAttributes" => {
                let name = queue_name(p, &self.config.context_path)?;
                let mut config = manager.queue(&name)?.clone();
                let attributes = string_map(p.get("Attributes"))?;
                for name in attributes.keys() {
                    validate_queue_attribute(name, false)?;
                }
                if let Some(value) = seconds_attribute(&attributes, "VisibilityTimeout", 43_200)? {
                    config.visibility_timeout_ms = value;
                }
                if let Some(value) = seconds_attribute(&attributes, "DelaySeconds", 900)? {
                    config.delay_ms = value;
                }
                if let Some(value) = seconds_attribute(
                    &attributes,
                    "ReceiveMessageWaitTimeSeconds",
                    self.wait_limit(),
                )? {
                    config.receive_wait_ms = value;
                }
                if attributes.contains_key("RedrivePolicy") {
                    config.dead_letter_queue = redrive(attributes.get("RedrivePolicy"), manager)?;
                }
                if let Some(seconds) = self.retention_attribute(&attributes)? {
                    config.message_retention_seconds = Some(seconds);
                }
                if let Some(bytes) = self.maximum_size_attribute(&attributes)? {
                    config.maximum_message_size = Some(bytes);
                }
                manager.update_queue(config)?;
                Ok(json!({}))
            }
            "TagQueue" => {
                manager.tag_queue(
                    &queue_name(p, &self.config.context_path)?,
                    string_map(p.get("Tags"))?,
                )?;
                Ok(json!({}))
            }
            "UntagQueue" => {
                manager.untag_queue(
                    &queue_name(p, &self.config.context_path)?,
                    &strings(p, "TagKeys")?,
                )?;
                Ok(json!({}))
            }
            "ListQueueTags" => {
                Ok(json!({"Tags":manager.queue(&queue_name(p, &self.config.context_path)?)?.tags}))
            }
            "ListDeadLetterSourceQueues" => {
                let name = queue_name(p, &self.config.context_path)?;
                manager.queue(&name)?;
                Ok(
                    json!({"queueUrls":manager.dead_letter_sources(&name).iter().map(|name| self.queue_url(name)).collect::<Vec<_>>()}),
                )
            }
            "StartMessageMoveTask" => {
                let source_arn = required(p, "SourceArn")?;
                let source = arn_queue(source_arn)?;
                let destination_arn = optional(p, "DestinationArn")?.map(str::to_owned);
                let destination = destination_arn.as_deref().map(arn_queue).transpose()?;
                let rate = if p.get("MaxNumberOfMessagesPerSecond").is_some() {
                    Some(integer(p, "MaxNumberOfMessagesPerSecond", None, 500)? as u32)
                } else {
                    None
                };
                let handle = manager.start_move_task(
                    StartMoveTask {
                        source,
                        destination,
                        source_arn: source_arn.into(),
                        destination_arn,
                        max_messages_per_second: rate,
                    },
                    now,
                )?;
                Ok(json!({"TaskHandle":handle}))
            }
            "CancelMessageMoveTask" => {
                let moved = manager.cancel_move_task(required(p, "TaskHandle")?)?;
                Ok(json!({"ApproximateNumberOfMessagesMoved":moved}))
            }
            "ListMessageMoveTasks" => {
                let tasks = manager.list_move_tasks(&arn_queue(required(p, "SourceArn")?)?)?;
                let results: Vec<_> = tasks.into_iter().map(|task| {
                    let mut result = json!({"TaskHandle":task.handle, "Status":task.status,
                        "ApproximateNumberOfMessagesMoved":task.moved,"ApproximateNumberOfMessagesToMove":task.to_move,
                        "StartedTimestamp":task.started,"SourceArn":task.source_arn});
                    if let Some(arn) = task.destination_arn { result["DestinationArn"] = json!(arn); }
                    if let Some(rate) = task.max_messages_per_second { result["MaxNumberOfMessagesPerSecond"] = json!(rate); }
                    if let Some(reason) = task.failure_reason { result["FailureReason"] = json!(reason); }
                    result
                }).collect();
                Ok(json!({"Results":results}))
            }
            _ => Err(ApiError::new(
                "InvalidAction",
                format!("Action {action} is not implemented"),
            )),
        }
    }

    async fn receive(&self, p: &Value) -> ApiResult<Value> {
        let name = queue_name(p, &self.config.context_path)?;
        let count = integer(
            p,
            "MaxNumberOfMessages",
            Some(1),
            if self.config.relaxed_limits {
                usize::MAX as u64
            } else {
                10
            },
        )?;
        if !self.config.relaxed_limits && count == 0 {
            return Err(ApiError::invalid(
                "MaxNumberOfMessages must be between 1 and 10",
            ));
        }
        let visibility = if p.get("VisibilityTimeout").is_some() {
            Some(integer(p, "VisibilityTimeout", None, 43_200)? * 1000)
        } else {
            None
        };
        // Parsed before locking; errors are still reported in the original order,
        // after the queue lookup.
        let attempt_id = optional(p, "ReceiveRequestAttemptId").map(|id| id.map(str::to_owned));
        let system_names = if p.get("MessageSystemAttributeNames").is_some() {
            strings(p, "MessageSystemAttributeNames")
        } else {
            strings(p, "AttributeNames")
        };
        let attribute_names = strings(p, "MessageAttributeNames");
        let mut changed = self.changed.subscribe();
        let mut stopping = self.stopping.subscribe();
        // The queue lookup and the first receive share one lock and one commit.
        // The writer commits in order, so that commit also covers any earlier
        // change, such as the queue being created here.
        let (setup, commit) = {
            let mut manager = self.manager.lock().await;
            let now = now();
            let setup = (|| -> ApiResult<_> {
                self.ensure_queue(&mut manager, &name, now)?;
                let wait_default = manager.queue(&name)?.receive_wait_ms / 1000;
                let wait = integer(p, "WaitTimeSeconds", Some(wait_default), self.wait_limit())?;
                let request = ReceiveMessages {
                    count: count as usize,
                    visibility_timeout_ms: visibility,
                    attempt_id: attempt_id?,
                };
                let names = (system_names?, attribute_names?);
                let deadline = tokio::time::Instant::now()
                    .checked_add(Duration::from_secs(wait))
                    .ok_or_else(|| ApiError::invalid("WaitTimeSeconds is too large"))?;
                let first = (!*stopping.borrow()).then(|| {
                    (
                        manager.receive_messages(&name, request.clone(), now),
                        manager.next_delivery(&name),
                    )
                });
                Ok((request, names, deadline, first))
            })();
            (setup, self.notify_change(&mut manager))
        };
        commit.wait().await?;
        let (request, (system_names, attribute_names), deadline, mut first) = setup?;
        loop {
            let (messages, next_delivery) = match first.take() {
                Some(result) => result,
                None => {
                    if *stopping.borrow() {
                        return Ok(json!({"Messages":[]}));
                    }
                    let (messages, next_delivery, commit) = {
                        let mut manager = self.manager.lock().await;
                        let messages = manager.receive_messages(&name, request.clone(), now());
                        let commit = self.notify_change(&mut manager);
                        (messages, manager.next_delivery(&name), commit)
                    };
                    commit.wait().await?;
                    (messages, next_delivery)
                }
            };
            let (messages, next_delivery) = (messages?, next_delivery?);
            if !messages.is_empty() || tokio::time::Instant::now() >= deadline {
                // Building a large response (MD5 and JSON) takes milliseconds,
                // ~10 ms for ten 256 KiB bodies. Without a database nothing
                // before this awaits, so the request would run in one poll
                // while tasks it woke wait in this worker's LIFO slot, where
                // other workers can't steal them. Yielding first lets them
                // run. Only for large responses: yielding on every request
                // cost small receives ~20% on Linux. Measured in containers:
                // 256 KiB receive and delete went from ~1,500 to ~2,500 msg/s.
                let bytes: usize = messages.iter().map(|m| m.body.len()).sum();
                if bytes >= YIELD_BEFORE_RESPONSE_BYTES {
                    tokio::task::yield_now().await;
                }
                return Ok(
                    json!({"Messages": messages.iter().map(|m| self.received(m, &system_names, &attribute_names)).collect::<Vec<_>>()}),
                );
            }
            let wake = next_delivery
                .map(|at| {
                    tokio::time::Instant::now()
                        + Duration::from_millis(at.saturating_sub(now()).max(1) as u64)
                })
                .unwrap_or(deadline)
                .min(deadline);
            tokio::select! {
                _ = stopping.changed() => {},
                _ = changed.changed() => {},
                _ = tokio::time::sleep_until(wake) => {},
            }
        }
    }

    fn received(
        &self,
        message: &Message,
        system_names: &[String],
        attribute_names: &[String],
    ) -> Value {
        // Build only the requested system attributes; most receives ask for none.
        let all = system_names.iter().any(|n| n == "All");
        let wanted = |key: &str| all || system_names.iter().any(|n| n == key);
        let mut system = BTreeMap::new();
        for (name, value) in [
            ("SentTimestamp", message.created),
            ("ApproximateReceiveCount", i64::from(message.receive_count)),
            (
                "ApproximateFirstReceiveTimestamp",
                message.first_receive.unwrap_or(0),
            ),
        ] {
            if wanted(name) {
                system.insert(name, value.to_string());
            }
        }
        if wanted("SenderId") {
            system.insert("SenderId", self.config.account_id.clone());
        }
        for (name, value) in [
            ("MessageGroupId", &message.group_id),
            ("MessageDeduplicationId", &message.deduplication_id),
            ("SequenceNumber", &message.sequence_number),
            ("AWSTraceHeader", &message.tracing_id),
        ] {
            if let Some(value) = value.as_ref().filter(|_| wanted(name)) {
                system.insert(name, value.clone());
            }
        }
        if let Some(source) = &message.dead_letter_source_queue
            && wanted("DeadLetterQueueSourceArn")
        {
            system.insert("DeadLetterQueueSourceArn", self.config.arn(source));
        }
        let attributes: Attributes = message
            .attributes
            .iter()
            .filter(|(key, _)| {
                attribute_names.iter().any(|name| {
                    name == "All"
                        || name == ".*"
                        || name == *key
                        // `prefix.*` matches names starting with `prefix.`
                        || name
                            .strip_suffix('*')
                            .is_some_and(|prefix| prefix.ends_with('.') && key.starts_with(prefix))
                })
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let mut result = json!({"MessageId":message.id, "ReceiptHandle":message.receipt_handle, "Body":message.body, "MD5OfBody":digest::body_digest(&message.body)});
        if !system.is_empty() {
            result["Attributes"] = json!(system);
        }
        if !attributes.is_empty() {
            result["MD5OfMessageAttributes"] = json!(digest::attribute_digest(&attributes));
            result["MessageAttributes"] = attributes_json(&attributes);
        }
        result
    }
}

async fn metrics(State(service): State<SqsService>) -> Response {
    use std::fmt::Write;
    let manager = service.manager.lock().await;
    let config = &service.config;
    let mut output = String::new();
    let mut family = |name: &str, kind: &str, help: &str, samples: &[(String, String)]| {
        let _ = writeln!(output, "# HELP {name} {help}\n# TYPE {name} {kind}");
        for (labels, value) in samples {
            let _ = writeln!(output, "{name}{labels} {value}");
        }
    };
    family(
        "rusticmq_build_info",
        "gauge",
        "Version and source commit of the running build",
        &[(
            format!(
                "{{version=\"{}\",commit=\"{}\"}}",
                env!("CARGO_PKG_VERSION"),
                env!("RUSTICMQ_COMMIT")
            ),
            "1".into(),
        )],
    );
    family(
        "rusticmq_config_info",
        "gauge",
        "Startup configuration file, storage and SQS limits mode",
        &[(
            format!(
                "{{file=\"{}\",storage=\"{}\",sqs_limits=\"{}\"}}",
                label_value(config.config_file.as_deref().unwrap_or("")),
                config.storage.label(),
                if config.relaxed_limits {
                    "relaxed"
                } else {
                    "strict"
                }
            ),
            "1".into(),
        )],
    );
    family(
        "process_start_time_seconds",
        "gauge",
        "Start time of the process since the Unix epoch",
        &[(String::new(), seconds(service.started))],
    );
    family(
        "rusticmq_persistence_errors_total",
        "counter",
        "Failed database commit attempts, each failing every request in its group",
        &[(
            String::new(),
            service
                .writer
                .as_ref()
                .map_or(0, |writer| writer.errors())
                .to_string(),
        )],
    );
    family(
        "rusticmq_requests_too_large_total",
        "counter",
        "Requests rejected for exceeding max-content-length",
        &service
            .too_large
            .counts()
            .map(|(action, count)| (format!("{{action=\"{action}\"}}"), count.to_string()))
            .collect::<Vec<_>>(),
    );
    family(
        "rusticmq_config_queues",
        "gauge",
        "Queues defined by the startup configuration",
        &[(String::new(), config.configured_queues.to_string())],
    );
    family(
        "rusticmq_config_max_content_length_bytes",
        "gauge",
        "Request body limit; +Inf when unlimited",
        &[(
            String::new(),
            config
                .max_content_length
                .map_or("+Inf".into(), |bytes| bytes.to_string()),
        )],
    );
    let time = now();
    let queues: Vec<_> = manager
        .list_queues(None)
        .into_iter()
        .filter_map(|name| {
            let stats = manager.statistics(&name, time).ok()?;
            let activity = manager.activity(&name).ok()?.clone();
            Some((format!("{{queue=\"{name}\"}}"), stats, activity))
        })
        .collect();
    let per_queue = |value: &dyn Fn(&QueueStatistics, &QueueActivity) -> Option<String>| {
        queues
            .iter()
            .filter_map(|(labels, stats, activity)| Some((labels.clone(), value(stats, activity)?)))
            .collect::<Vec<_>>()
    };
    for (name, kind, help, samples) in [
        (
            "rusticmq_messages_visible",
            "gauge",
            "Visible messages",
            per_queue(&|stats, _| Some(stats.visible.to_string())),
        ),
        (
            "rusticmq_messages_invisible",
            "gauge",
            "Messages awaiting visibility timeout",
            per_queue(&|stats, _| Some(stats.invisible.to_string())),
        ),
        (
            "rusticmq_messages_delayed",
            "gauge",
            "Messages awaiting their initial delivery",
            per_queue(&|stats, _| Some(stats.delayed.to_string())),
        ),
        (
            "rusticmq_messages_sent_total",
            "counter",
            "Messages stored by sends, including copy/move routing; not dead-letter or move-task arrivals",
            per_queue(&|_, activity| Some(activity.sent.to_string())),
        ),
        (
            "rusticmq_messages_deduplicated_total",
            "counter",
            "FIFO sends accepted but not stored as duplicates within five minutes",
            per_queue(&|_, activity| Some(activity.deduplicated.to_string())),
        ),
        (
            "rusticmq_messages_received_total",
            "counter",
            "Messages delivered by receives",
            per_queue(&|_, activity| Some(activity.received.to_string())),
        ),
        (
            "rusticmq_messages_deleted_total",
            "counter",
            "Messages deleted by receipt handle",
            per_queue(&|_, activity| Some(activity.deleted.to_string())),
        ),
        (
            "rusticmq_messages_expired_total",
            "counter",
            "Messages deleted for exceeding the queue's retention period",
            per_queue(&|_, activity| Some(activity.expired.to_string())),
        ),
        (
            "rusticmq_queue_last_send_timestamp_seconds",
            "gauge",
            "Time of the last send to the queue, stored or deduplicated",
            per_queue(&|_, activity| activity.last_send.map(seconds)),
        ),
        (
            "rusticmq_queue_last_receive_timestamp_seconds",
            "gauge",
            "Time of the last receive request, including empty receives and long-poll wakeups",
            per_queue(&|_, activity| activity.last_receive.map(seconds)),
        ),
    ] {
        family(name, kind, help, &samples);
    }
    // `family` appends the label text to the name, so it also carries the
    // histogram's `_bucket`, `_sum` and `_count` suffixes.
    let mut histogram = Vec::new();
    for (labels, _, activity) in &queues {
        let queue = labels.trim_start_matches('{').trim_end_matches('}');
        let mut cumulative = 0;
        for (bound, count) in BODY_BYTES_BUCKETS.iter().zip(activity.body_bytes_buckets) {
            cumulative += count;
            histogram.push((
                format!("_bucket{{{queue},le=\"{bound}\"}}"),
                cumulative.to_string(),
            ));
        }
        histogram.push((
            format!("_bucket{{{queue},le=\"+Inf\"}}"),
            activity.sent.to_string(),
        ));
        histogram.push((format!("_sum{labels}"), activity.body_bytes_sum.to_string()));
        histogram.push((format!("_count{labels}"), activity.sent.to_string()));
    }
    family(
        "rusticmq_message_body_bytes",
        "histogram",
        "Sizes of stored message bodies in UTF-8 bytes",
        &histogram,
    );
    (
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        output,
    )
        .into_response()
}

impl SqsService {
    /// Counts and logs an unreadable body, returning an SQS-style error in
    /// place of axum's plain-text rejection, which SDKs cannot parse.
    fn rejected(&self, rejection: BytesRejection, uri: &Uri, headers: &HeaderMap) -> ApiError {
        let action = headers
            .get("x-amz-target")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.rsplit('.').next())
            .map(str::to_owned)
            .or_else(|| {
                serde_urlencoded::from_str::<BTreeMap<String, String>>(uri.query()?)
                    .ok()?
                    .remove("Action")
            })
            .unwrap_or_else(|| "unknown".into());
        if rejection.status() != StatusCode::PAYLOAD_TOO_LARGE {
            return ApiError::invalid(rejection.body_text());
        }
        self.too_large.record(&action);
        let length = headers
            .get("content-length")
            .and_then(|h| h.to_str().ok())
            .map_or("unknown".into(), |length| format!("{length} bytes"));
        let limit = self
            .config
            .max_content_length
            .map_or("unlimited".into(), |bytes| bytes.to_string());
        eprintln!(
            "Rejected {action} {}: body of {length} exceeds max-content-length ({limit} bytes)",
            uri.path()
        );
        ApiError::new(
            "RequestEntityTooLarge",
            format!("The request body exceeds max-content-length ({limit} bytes)"),
        )
    }
}

fn seconds(millis: Timestamp) -> String {
    format!("{}.{:03}", millis / 1000, millis % 1000)
}

fn label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

async fn handle(State(mut service): State<SqsService>, request: Request) -> Response {
    // Take the request apart instead of using header and URI extractors, which
    // clone them. The body extractor still applies the configured size limit.
    let (mut parts, body) = request.into_parts();
    let headers = std::mem::take(&mut parts.headers);
    let method = parts.method.clone();
    let uri = parts
        .extensions
        .get::<OriginalUri>()
        .map_or_else(|| parts.uri.clone(), |original| original.0.clone());
    let body = Bytes::from_request(Request::from_parts(parts, body), &()).await;
    if method == Method::OPTIONS {
        return cors((StatusCode::NO_CONTENT, "").into_response());
    }
    let json_protocol = headers.contains_key("x-amz-target")
        || headers
            .get("content-type")
            .and_then(|h| h.to_str().ok())
            .is_some_and(|h| h.contains("json"));
    let (body, rejected) = match body {
        Ok(body) => (body, None),
        Err(rejection) => (
            Bytes::new(),
            Some(service.rejected(rejection, &uri, &headers)),
        ),
    };
    if service.config.wildcard_queue_urls {
        // The request's host and port, followed by the configured context path.
        // The request path is ignored: requests may target the root or a queue.
        let protocol = uri.scheme_str().unwrap_or("http");
        let authority = uri
            .authority()
            .map(|value| value.as_str())
            .or_else(|| headers.get("host").and_then(|value| value.to_str().ok()))
            .unwrap_or("localhost");
        service.request_base_url =
            Some(format!("{protocol}://{authority}{}", service.config.context_path).into());
    }
    let parsed = if let Some(error) = rejected {
        Err(error)
    } else if json_protocol {
        let action = headers
            .get("x-amz-target")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.rsplit('.').next())
            .unwrap_or("")
            .to_owned();
        if action.is_empty() {
            Err(ApiError::new("MissingAction", "X-Amz-Target is missing"))
        } else {
            serde_json::from_slice::<Value>(&body)
                .map_err(|_| ApiError::invalid("Invalid JSON request"))
                .and_then(|value| {
                    if !value.is_object() {
                        return Err(ApiError::invalid("JSON request must be an object"));
                    }
                    Ok((action, value))
                })
        }
    } else {
        let query =
            serde_urlencoded::from_str::<BTreeMap<String, String>>(uri.query().unwrap_or(""));
        query
            .and_then(|mut query| {
                let body = serde_urlencoded::from_bytes::<BTreeMap<String, String>>(&body)?;
                query.extend(body);
                Ok(query)
            })
            .map_err(|_| ApiError::invalid("Invalid query parameters"))
            .and_then(protocol::query_request)
    };
    let mut request_id = [0u8; uuid::fmt::Hyphenated::LENGTH];
    let request_id: &str = uuid::Uuid::new_v4()
        .hyphenated()
        .encode_lower(&mut request_id);
    let action = parsed
        .as_ref()
        .map(|(action, _)| action.clone())
        .unwrap_or_default();
    let result = match parsed {
        Ok((action, mut request)) => {
            if request.get("QueueUrl").is_none() && uri.path() != "/" {
                request["QueueUrl"] = json!(uri.path());
            }
            let tracing = headers
                .get("x-amzn-trace-id")
                .and_then(|h| h.to_str().ok())
                .map(str::to_owned);
            service.execute(&action, request, tracing).await
        }
        Err(error) => Err(error),
    };
    let error_type = result.as_ref().err().map(ApiError::json_type);
    let (status, text) = match result {
        Ok(value) => (
            StatusCode::OK,
            if json_protocol {
                json_text(&value)
            } else {
                protocol::xml_response(&action, &value, request_id)
            },
        ),
        Err(error) => (
            if error.code == "InternalError" {
                StatusCode::INTERNAL_SERVER_ERROR
            } else if error.code == "RequestEntityTooLarge" {
                StatusCode::PAYLOAD_TOO_LARGE
            } else {
                StatusCode::BAD_REQUEST
            },
            if json_protocol {
                json!({"__type":error.json_type(),"Message":error.message}).to_string()
            } else {
                protocol::xml_error(&error, request_id)
            },
        ),
    };
    let mut response = (status, text).into_response();
    if json_protocol && let Some(error_type) = error_type {
        response.headers_mut().insert(
            "x-amzn-query-error",
            error_type.parse().expect("constant error type"),
        );
    }
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static(if json_protocol {
            "application/x-amz-json-1.0"
        } else {
            "text/xml; charset=utf-8"
        }),
    );
    response.headers_mut().insert(
        "x-amzn-requestid",
        HeaderValue::from_str(request_id).expect("UUID header value"),
    );
    cors(response)
}

/// Serialize a JSON response into one buffer sized for typical responses.
fn json_text(value: &Value) -> String {
    let mut buffer = Vec::with_capacity(1024);
    serde_json::to_writer(&mut buffer, value).expect("JSON values serialize");
    String::from_utf8(buffer).expect("serde_json writes UTF-8")
}

fn cors(mut response: Response) -> Response {
    let headers = response.headers_mut();
    headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
    headers.insert(ACCESS_CONTROL_ALLOW_HEADERS, HeaderValue::from_static("*"));
    headers.insert(
        ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, POST, OPTIONS"),
    );
    response
}

pub fn now() -> Timestamp {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

/// Validate a batch request's entries; these checks do not depend on the queue.
fn batch_entries(p: &Value, relaxed: bool) -> ApiResult<&Vec<Value>> {
    let entries = p
        .get("Entries")
        .and_then(Value::as_array)
        .ok_or_else(|| ApiError::new("MissingParameter", "Entries is required"))?;
    if entries.is_empty() {
        return Err(ApiError::new(
            "EmptyBatchRequest",
            "Batch must contain entries",
        ));
    }
    if !relaxed && entries.len() > 10 {
        return Err(ApiError::new(
            "TooManyEntriesInBatchRequest",
            "Batch cannot exceed 10 entries",
        ));
    }
    let mut ids = BTreeSet::new();
    for entry in entries {
        let id = required(entry, "Id")?;
        if id.is_empty()
            || id.len() > 80
            || !id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
        {
            return Err(ApiError::new(
                "InvalidBatchEntryId",
                "Invalid batch entry ID",
            ));
        }
        if !ids.insert(id) {
            return Err(ApiError::new(
                "BatchEntryIdsNotDistinct",
                "Batch entry IDs must be distinct",
            ));
        }
    }
    Ok(entries)
}

/// A SendMessage request parsed and validated without the engine lock. Each
/// group of checks keeps its result, so that under the lock errors surface in
/// the same order as when everything ran there: queue-dependent FIFO checks
/// sit between the identifier and delay checks.
struct PreparedSend {
    content: ApiResult<(MessageBody, Attributes)>,
    ids: ApiResult<(Option<String>, Option<String>)>,
    delay: ApiResult<Option<u64>>,
    tracing: ApiResult<Option<String>>,
    body_digest: String,
    attribute_digest: Option<String>,
    /// The message size in bytes as SQS counts it, for the queue's limit.
    size: u64,
}

/// SQS's message size limit, and its default `MaximumMessageSize`.
const STRICT_MAXIMUM_MESSAGE_SIZE: u32 = 1024 * 1024;

/// The size of a message as SQS counts it: the body, plus each attribute's
/// name, data type and value, in UTF-8 bytes.
fn message_size(body: &str, attributes: &Attributes) -> u64 {
    let attributes: usize = attributes
        .iter()
        .map(|(name, attribute)| {
            name.len()
                + attribute.data_type().len()
                + match &attribute.value {
                    AttributeValue::String(text) | AttributeValue::Number(text) => text.len(),
                    AttributeValue::Binary(bytes) => bytes.len(),
                }
        })
        .sum();
    (body.len() + attributes) as u64
}

fn prepare_send(p: &Value, tracing: Option<String>, relaxed: bool) -> PreparedSend {
    let content = (|| {
        let body = required(p, "MessageBody")?;
        validate_text(body, relaxed)?;
        let attributes = parse_message_attributes(p.get("MessageAttributes"), relaxed)?;
        if !relaxed && attributes.len() > 10 {
            return Err(ApiError::new(
                "InvalidAttributeValue",
                "At most 10 message attributes are allowed",
            ));
        }
        Ok((MessageBody::from(body), attributes))
    })();
    let ids = (|| {
        let group_id = optional(p, "MessageGroupId")?.map(str::to_owned);
        let deduplication_id = optional(p, "MessageDeduplicationId")?.map(str::to_owned);
        for (parameter, id) in [
            ("MessageGroupId", &group_id),
            ("MessageDeduplicationId", &deduplication_id),
        ] {
            let Some(id) = id else {
                continue;
            };
            if id.is_empty() || id.len() > 128 || !id.bytes().all(|c| c.is_ascii_graphic()) {
                return Err(ApiError::invalid(format!(
                    "{parameter} can only include alphanumeric and punctuation characters. 1 to 128 in length."
                )));
            }
        }
        Ok((group_id, deduplication_id))
    })();
    let delay = if p.get("DelaySeconds").is_some() {
        integer(p, "DelaySeconds", None, 900).map(Some)
    } else {
        Ok(None)
    };
    let tracing =
        parse_message_attributes(p.get("MessageSystemAttributes"), relaxed).and_then(|system| {
            match system.get("AWSTraceHeader") {
                Some(MessageAttribute {
                    value: AttributeValue::String(value),
                    ..
                }) => Ok(Some(value.clone())),
                Some(_) => Err(ApiError::invalid("AWSTraceHeader must be a String")),
                None => Ok(tracing),
            }
        });
    let size = match &content {
        Ok((body, attributes)) => message_size(body, attributes),
        Err(_) => p["MessageBody"]
            .as_str()
            .map_or(0, |body| body.len() as u64),
    };
    let (body_digest, attribute_digest) = match &content {
        Ok((body, attributes)) => (
            digest::body_digest(body),
            (!attributes.is_empty()).then(|| digest::attribute_digest(attributes)),
        ),
        Err(_) => (String::new(), None),
    };
    PreparedSend {
        content,
        ids,
        delay,
        tracing,
        body_digest,
        attribute_digest,
        size,
    }
}

/// A stored (or deduplicated) message, for the response built after the lock.
struct Sent {
    id: String,
    sequence_number: Option<String>,
    body_digest: String,
    attribute_digest: Option<String>,
}

impl Sent {
    fn into_json(self) -> Value {
        let mut result = json!({"MessageId":self.id, "MD5OfMessageBody":self.body_digest});
        if let Some(digest) = self.attribute_digest {
            result["MD5OfMessageAttributes"] = json!(digest);
        }
        if let Some(sequence) = self.sequence_number {
            result["SequenceNumber"] = json!(sequence);
        }
        result
    }
}

struct PreparedBatch {
    name: ApiResult<String>,
    /// Entry IDs with their prepared sends, or the batch-level validation error.
    entries: ApiResult<Vec<(Value, PreparedSend)>>,
}

struct SentBatch(Vec<(Value, ApiResult<Sent>)>);

impl SentBatch {
    fn into_json(self) -> Value {
        let mut successful = Vec::new();
        let mut failed = Vec::new();
        for (id, result) in self.0 {
            match result {
                Ok(sent) => {
                    let mut result = sent.into_json();
                    result["Id"] = id;
                    successful.push(result);
                }
                Err(error) => failed.push(json!({"Id":id, "Code":error.code, "Message":error.message, "SenderFault":true})),
            }
        }
        json!({"Successful":successful, "Failed":failed})
    }
}

fn required<'a>(p: &'a Value, key: &str) -> ApiResult<&'a str> {
    optional(p, key)?.ok_or_else(|| {
        ApiError::new(
            "MissingParameter",
            format!("The request must contain the parameter {key}."),
        )
    })
}
fn optional<'a>(p: &'a Value, key: &str) -> ApiResult<Option<&'a str>> {
    match p.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        _ => Err(ApiError::invalid(format!("{key} must be a string"))),
    }
}
fn integer(p: &Value, key: &str, default: Option<u64>, max: u64) -> ApiResult<u64> {
    let value = match p.get(key) {
        None | Some(Value::Null) => default
            .ok_or_else(|| ApiError::new("MissingParameter", format!("{key} is required")))?,
        Some(value) => value
            .as_u64()
            .ok_or_else(|| ApiError::invalid(format!("{key} must be a nonnegative integer")))?,
    };
    if value > max {
        return Err(ApiError::invalid(format!(
            "{key} must be between 0 and {max}"
        )));
    }
    Ok(value)
}
fn strings(p: &Value, key: &str) -> ApiResult<Vec<String>> {
    match p.get(key) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(values)) => values
            .iter()
            .map(|v| {
                v.as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| ApiError::invalid(format!("{key} must contain strings")))
            })
            .collect(),
        _ => Err(ApiError::invalid(format!("{key} must be an array"))),
    }
}
fn string_map(value: Option<&Value>) -> ApiResult<BTreeMap<String, String>> {
    match value {
        None | Some(Value::Null) => Ok(BTreeMap::new()),
        Some(Value::Object(values)) => values
            .iter()
            .map(|(k, v)| {
                v.as_str()
                    .map(|v| (k.clone(), v.to_owned()))
                    .ok_or_else(|| ApiError::invalid(format!("{k} must be a string")))
            })
            .collect(),
        _ => Err(ApiError::invalid("Expected an object of string values")),
    }
}
fn queue_name(p: &Value, context: &str) -> ApiResult<String> {
    let url = required(p, "QueueUrl")?;
    let path = url
        .parse::<Uri>()
        .map_err(|_| ApiError::invalid("Invalid QueueUrl"))?;
    let path = path
        .path()
        .strip_prefix(context.trim_end_matches('/'))
        .and_then(|path| path.strip_prefix('/'))
        .ok_or_else(|| ApiError::invalid("QueueUrl must include the configured context path"))?;
    let mut parts = path.trim_end_matches('/').split('/');
    let name = match (parts.next(), parts.next(), parts.next()) {
        (Some(name), None, _) => name,
        (Some(account), Some(name), None)
            if !account.is_empty() && account.bytes().all(|c| c.is_ascii_alphanumeric()) =>
        {
            name
        }
        _ => {
            return Err(ApiError::invalid(
                "QueueUrl must use /<accountId>/<queueName> or /<queueName>",
            ));
        }
    };
    if name.is_empty() {
        return Err(ApiError::invalid("Invalid QueueUrl"));
    }
    Ok(name.to_owned())
}
fn arn_queue(arn: &str) -> ApiResult<String> {
    let parts: Vec<_> = arn.split(':').collect();
    if parts.len() != 6
        || parts[0] != "arn"
        || parts[1] != "aws"
        || parts[2] != "sqs"
        || parts[5].is_empty()
    {
        return Err(Error::QueueDoesNotExist(arn.into()).into());
    }
    Ok(parts[5].into())
}

fn boolean(attributes: &BTreeMap<String, String>, name: &str) -> ApiResult<Option<bool>> {
    attributes
        .get(name)
        .map(|value| {
            value.parse::<bool>().map_err(|_| {
                ApiError::new(
                    "InvalidAttributeValue",
                    format!("{name} must be true or false"),
                )
            })
        })
        .transpose()
}
/// A positive integer attribute, within `range` when given and at most `max`.
fn positive_attribute(
    attributes: &BTreeMap<String, String>,
    name: &str,
    range: Option<std::ops::RangeInclusive<u64>>,
    max: u64,
) -> ApiResult<Option<u64>> {
    attributes
        .get(name)
        .map(|value| {
            value
                .parse::<u64>()
                .ok()
                .filter(|number| {
                    (1..=max).contains(number)
                        && range.as_ref().is_none_or(|range| range.contains(number))
                })
                .ok_or_else(|| {
                    ApiError::new(
                        "InvalidAttributeValue",
                        format!("Invalid value for the parameter {name}: {value}."),
                    )
                })
        })
        .transpose()
}
fn seconds_attribute(
    attributes: &BTreeMap<String, String>,
    name: &str,
    max: u64,
) -> ApiResult<Option<u64>> {
    attributes
        .get(name)
        .map(|value| {
            let value = value
                .parse::<u64>()
                .map_err(|_| ApiError::new("InvalidAttributeValue", format!("Invalid {name}")))?;
            if value > max {
                return Err(ApiError::new(
                    "InvalidAttributeValue",
                    format!("{name} exceeds {max}"),
                ));
            }
            Ok(value * 1000)
        })
        .transpose()
}
fn validate_queue_name_wire(name: &str, fifo: bool, relaxed: bool) -> ApiResult<()> {
    let base = if fifo {
        name.strip_suffix(".fifo").unwrap_or("")
    } else {
        name
    };
    if base.is_empty()
        || !base
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
    {
        return Err(ApiError::new(
            "InvalidAttributeName",
            if fifo {
                "The name of a FIFO queue can only include alphanumeric characters, hyphens, or underscores, must end with .fifo suffix."
            } else {
                "Can only include alphanumeric characters, hyphens, or underscores."
            },
        ));
    }
    if !relaxed && name.len() > 80 {
        return Err(ApiError::new(
            "InvalidAttributeName",
            "The name of a queue must not be longer than 80.",
        ));
    }
    Ok(())
}

fn validate_queue_attribute(name: &str, create: bool) -> ApiResult<()> {
    if matches!(
        name,
        "VisibilityTimeout"
            | "DelaySeconds"
            | "ReceiveMessageWaitTimeSeconds"
            | "RedrivePolicy"
            | "Policy"
            | "MaximumMessageSize"
            | "MessageRetentionPeriod"
            | "FifoQueue"
    ) || (create
        && matches!(
            name,
            "ContentBasedDeduplication"
                | "ApproximateNumberOfMessages"
                | "ApproximateNumberOfMessagesNotVisible"
                | "ApproximateNumberOfMessagesDelayed"
                | "CreatedTimestamp"
                | "LastModifiedTimestamp"
                | "QueueArn"
        ))
    {
        return Ok(());
    }
    Err(ApiError::new(
        "InvalidAttributeName",
        format!("The attribute {name} is invalid"),
    ))
}
fn redrive(value: Option<&String>, manager: &QueueManager) -> ApiResult<Option<DeadLetterQueue>> {
    let Some(value) = value else { return Ok(None) };
    let value: Value = serde_json::from_str(value)
        .map_err(|_| ApiError::new("InvalidAttributeValue", "Invalid RedrivePolicy"))?;
    let arn = required(&value, "deadLetterTargetArn")?;
    let name = arn
        .rsplit(':')
        .next()
        .filter(|name| !name.is_empty())
        .ok_or_else(|| ApiError::invalid("Invalid dead-letter ARN"))?;
    manager.queue(name)?;
    let count = value
        .get("maxReceiveCount")
        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
        .filter(|count| (1..=1000).contains(count))
        .ok_or_else(|| ApiError::invalid("maxReceiveCount must be between 1 and 1000"))?;
    Ok(Some(DeadLetterQueue {
        name: name.into(),
        max_receive_count: count as u32,
    }))
}
fn validate_text(text: &str, relaxed: bool) -> ApiResult<()> {
    if relaxed {
        return Ok(());
    }
    let error = if text.is_empty() {
        Some("The request must contain the parameter MessageBody.")
    } else if !text.chars().all(
        |c| matches!(c as u32, 9 | 10 | 13 | 0x20..=0xd7ff | 0xe000..=0xfffd | 0x10000..=0x10ffff),
    ) {
        Some("InvalidMessageContents")
    } else {
        None
    };
    if let Some(error) = error {
        return Err(ApiError::new("InvalidAttributeValue", error));
    }
    Ok(())
}
fn parse_message_attributes(value: Option<&Value>, relaxed: bool) -> ApiResult<Attributes> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(BTreeMap::new());
    };
    let object = value
        .as_object()
        .ok_or_else(|| ApiError::invalid("MessageAttributes must be an object"))?;
    object
        .iter()
        .map(|(name, attribute)| {
            let data_type = required(attribute, "DataType")?;
            let (base, custom) = data_type
                .split_once('.')
                .map(|(base, custom)| (base, Some(custom.to_owned())))
                .unwrap_or((data_type, None));
            let custom = custom.filter(|value| !value.is_empty());
            let value = match base {
                "String" => {
                    let value = required(attribute, "StringValue")?;
                    if !relaxed && value.is_empty() {
                        return Err(ApiError::new("InvalidAttributeValue", format!("Attribute '{name}' must contain a non-empty value of type 'String'")));
                    }
                    validate_text(value, relaxed)?;
                    AttributeValue::String(value.into())
                }
                "Number" => {
                    let value = required(attribute, "StringValue")?;
                    if !relaxed {
                        if value.is_empty() { return Err(ApiError::new("InvalidAttributeValue", format!("Attribute '{name}' must contain a non-empty value of type 'Number'"))); }
                        let number = value
                            .parse::<BigDecimal>()
                            .map_err(|_| ApiError::invalid("Invalid number attribute"))?;
                        let min: BigDecimal = "-1e128".parse().expect("constant decimal");
                        let max: BigDecimal = "1e126".parse().expect("constant decimal");
                        if number < min || number > max {
                            return Err(ApiError::invalid(
                                "Number attribute outside supported range",
                            ));
                        }
                    }
                    AttributeValue::Number(value.into())
                }
                "Binary" => AttributeValue::Binary(match attribute.get("BinaryValue") {
                    Some(Value::Array(values)) => values.iter().map(|value| {
                        value.as_i64().and_then(|value| i8::try_from(value).ok()).map(|value| value as u8)
                            .ok_or_else(|| ApiError::invalid("BinaryValue array must contain signed bytes"))
                    }).collect::<ApiResult<Vec<_>>>()?,
                    _ => STANDARD.decode(required(attribute, "BinaryValue")?)
                        .map_err(|_| ApiError::invalid("Invalid base64 attribute"))?,
                }),
                _ => {
                    return Err(ApiError::invalid(
                        "Message attribute type must be String, Number or Binary",
                    ));
                }
            };
            Ok((
                name.clone(),
                MessageAttribute {
                    value,
                    custom_type: custom,
                },
            ))
        })
        .collect()
}
fn attributes_json(attributes: &Attributes) -> Value {
    let values: serde_json::Map<String, Value> = attributes
        .iter()
        .map(|(name, attribute)| {
            let mut value = json!({"DataType":attribute.data_type()});
            match &attribute.value {
                AttributeValue::String(text) | AttributeValue::Number(text) => {
                    value["StringValue"] = json!(text);
                }
                AttributeValue::Binary(bytes) => {
                    value["BinaryValue"] = json!(STANDARD.encode(bytes));
                }
            }
            (name.clone(), value)
        })
        .collect();
    Value::Object(values)
}
