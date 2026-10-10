//! SQS wire protocols and HTTP service. Authentication is deliberately ignored,
//! matching ElasticMQ's local development service.
pub mod digest;
mod protocol;

use crate::*;
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, OriginalUri, State},
    http::{HeaderMap, Method, StatusCode, Uri},
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
use tokio::sync::{Mutex, watch};

type ApiResult<T> = Result<T, ApiError>;

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
            name => name,
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
        }
    }
}

impl ServerConfig {
    fn queue_url(&self, name: &str) -> String {
        format!(
            "{}/{}/{name}",
            self.base_url.trim_end_matches('/'),
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
    pub config: ServerConfig,
    changed: watch::Sender<u64>,
    persistence: Option<Arc<crate::persistence::Persistence>>,
}

impl SqsService {
    pub fn new(mut manager: QueueManager, config: ServerConfig) -> Self {
        manager.set_relaxed_limits(config.relaxed_limits);
        let (changed, _) = watch::channel(0);
        Self {
            manager: Arc::new(Mutex::new(manager)),
            config,
            changed,
            persistence: None,
        }
    }

    pub fn router(self) -> Router {
        let router = Router::new()
            .route("/health", get(|| async { "OK" }))
            .route("/metrics", get(metrics));
        let router = if self.config.context_path.is_empty() {
            router.fallback(handle)
        } else {
            router.nest(&self.config.context_path, Router::new().fallback(handle))
        };
        router
            .layer(DefaultBodyLimit::max(8 * 1024 * 1024))
            .with_state(self)
    }

    pub fn with_persistence(mut self, persistence: crate::persistence::Persistence) -> Self {
        self.persistence = Some(Arc::new(persistence));
        self
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
        let mut manager = self.manager.lock().await;
        let result = self.dispatch(&mut manager, action, &request, tracing, now());
        self.notify_change(&mut manager)?;
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
        result
    }

    fn notify_change(&self, manager: &mut QueueManager) -> ApiResult<()> {
        let events = manager.drain_events();
        if let Some(persistence) = &self.persistence
            && let Err(error) = persistence.apply(&events, manager)
        {
            manager.requeue_events(events);
            return Err(ApiError::new("InternalError", error.to_string()));
        }
        if !events.is_empty() {
            self.changed
                .send_modify(|version| *version = version.wrapping_add(1));
        }
        Ok(())
    }

    async fn run_move_task(&self, handle: String) {
        loop {
            let step = {
                let mut manager = self.manager.lock().await;
                if let Err(error) = self.notify_change(&mut manager) {
                    eprintln!("Message move task {handle} persistence: {}", error.message);
                    drop(manager);
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    continue;
                }
                let step = manager.step_move_task(&handle, now());
                if let Err(error) = self.notify_change(&mut manager) {
                    eprintln!("Message move task {handle} persistence: {}", error.message);
                    drop(manager);
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    continue;
                }
                step
            };
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
                request.tags = string_map(p.get("tags").or_else(|| p.get("Tags")))?;
                for key in attributes.keys() {
                    validate_queue_attribute(key, true)?;
                }
                let queue = manager.create_queue(request, now)?;
                Ok(json!({"QueueUrl": self.config.queue_url(&queue.name)}))
            }
            "ListQueues" => Ok(
                json!({"QueueUrls": manager.list_queues(optional(p, "QueueNamePrefix")?).iter().map(|name| self.config.queue_url(name)).collect::<Vec<_>>()}),
            ),
            "GetQueueUrl" => {
                let name = required(p, "QueueName")?;
                manager.queue(name)?;
                Ok(json!({"QueueUrl": self.config.queue_url(name)}))
            }
            "AddPermission" | "RemovePermission" => Ok(json!({})),
            "SendMessageBatch" | "DeleteMessageBatch" | "ChangeMessageVisibilityBatch" => {
                let name = queue_name(p, &self.config.context_path)?;
                manager.queue(&name)?;
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
                if !self.config.relaxed_limits && entries.len() > 10 {
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
                if action == "SendMessageBatch" && !self.config.relaxed_limits {
                    let mut length = 0usize;
                    for entry in entries {
                        length = length
                            .saturating_add(required(entry, "MessageBody")?.encode_utf16().count());
                    }
                    if length > 1024 * 1024 {
                        return Err(ApiError::new(
                            "InvalidAttributeValue",
                            "Batch message bodies exceed the maximum length",
                        ));
                    }
                }
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
                let config = manager.queue(&name)?.clone();
                let body = required(p, "MessageBody")?;
                validate_text(body, self.config.relaxed_limits)?;
                let attributes = parse_message_attributes(
                    p.get("MessageAttributes"),
                    self.config.relaxed_limits,
                )?;
                if !self.config.relaxed_limits && attributes.len() > 10 {
                    return Err(ApiError::new(
                        "InvalidAttributeValue",
                        "At most 10 message attributes are allowed",
                    ));
                }
                let group_id = optional(p, "MessageGroupId")?.map(str::to_owned);
                let deduplication_id = optional(p, "MessageDeduplicationId")?.map(str::to_owned);
                for (parameter, id) in [
                    ("MessageGroupId", &group_id),
                    ("MessageDeduplicationId", &deduplication_id),
                ] {
                    let Some(id) = id else {
                        continue;
                    };
                    if id.is_empty() || id.len() > 128 || !id.bytes().all(|c| c.is_ascii_graphic())
                    {
                        return Err(ApiError::invalid(format!(
                            "{parameter} can only include alphanumeric and punctuation characters. 1 to 128 in length."
                        )));
                    }
                }
                if config.fifo && group_id.is_none() {
                    return Err(ApiError::new(
                        "MissingParameter",
                        "MessageGroupId is required",
                    ));
                }
                if !config.fifo && deduplication_id.is_some() {
                    return Err(ApiError::invalid(
                        "MessageDeduplicationId is only valid for FIFO queues",
                    ));
                }
                let delay = if p.get("DelaySeconds").is_some() {
                    Some(integer(p, "DelaySeconds", None, 900)?)
                } else {
                    None
                };
                if config.fifo && delay.is_some_and(|seconds| seconds > 0) {
                    return Err(ApiError::invalid(
                        "The request includes parameter DelaySeconds that is not valid for this queue type",
                    ));
                }
                let system = parse_message_attributes(
                    p.get("MessageSystemAttributes"),
                    self.config.relaxed_limits,
                )?;
                let tracing_id = match system.get("AWSTraceHeader") {
                    Some(MessageAttribute {
                        value: AttributeValue::String(value),
                        ..
                    }) => Some(value.clone()),
                    Some(_) => return Err(ApiError::invalid("AWSTraceHeader must be a String")),
                    None => tracing,
                };
                let body_digest = digest::body_digest(body);
                let attribute_digest =
                    (!attributes.is_empty()).then(|| digest::attribute_digest(&attributes));
                let message = manager.send_message(
                    &name,
                    SendMessage {
                        body: body.into(),
                        attributes,
                        group_id,
                        deduplication_id,
                        delay_ms: delay.filter(|_| !config.fifo).map(|seconds| seconds * 1000),
                        tracing_id,
                        ..Default::default()
                    },
                    now,
                )?;
                let mut result = json!({"MessageId":message.id, "MD5OfMessageBody":body_digest});
                if let Some(digest) = attribute_digest {
                    result["MD5OfMessageAttributes"] = json!(digest);
                }
                if let Some(sequence) = message.sequence_number {
                    result["SequenceNumber"] = json!(sequence);
                }
                Ok(result)
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
                    json!({"queueUrls":manager.dead_letter_sources(&name).iter().map(|name| self.config.queue_url(name)).collect::<Vec<_>>()}),
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
        let wait_default = {
            let mut manager = self.manager.lock().await;
            self.ensure_queue(&mut manager, &name, now())?;
            self.notify_change(&mut manager)?;
            manager.queue(&name)?.receive_wait_ms / 1000
        };
        let wait = integer(p, "WaitTimeSeconds", Some(wait_default), self.wait_limit())?;
        let attempt_id = optional(p, "ReceiveRequestAttemptId")?.map(str::to_owned);
        let request = ReceiveMessages {
            count: count as usize,
            visibility_timeout_ms: visibility,
            attempt_id,
        };
        let system_names = if p.get("MessageSystemAttributeNames").is_some() {
            strings(p, "MessageSystemAttributeNames")?
        } else {
            strings(p, "AttributeNames")?
        };
        let attribute_names = strings(p, "MessageAttributeNames")?;
        let deadline = tokio::time::Instant::now()
            .checked_add(Duration::from_secs(wait))
            .ok_or_else(|| ApiError::invalid("WaitTimeSeconds is too large"))?;
        let mut changed = self.changed.subscribe();
        loop {
            let messages = {
                let mut manager = self.manager.lock().await;
                let messages = manager.receive_messages(&name, request.clone(), now());
                self.notify_change(&mut manager)?;
                messages?
            };
            if !messages.is_empty() || tokio::time::Instant::now() >= deadline {
                return Ok(
                    json!({"Messages": messages.iter().map(|m| self.received(m, &system_names, &attribute_names)).collect::<Vec<_>>()}),
                );
            }
            tokio::select! {
                _ = changed.changed() => {},
                _ = tokio::time::sleep_until(deadline.min(tokio::time::Instant::now() + Duration::from_millis(25))) => {},
            }
        }
    }

    fn received(
        &self,
        message: &Message,
        system_names: &[String],
        attribute_names: &[String],
    ) -> Value {
        let mut system = BTreeMap::from([
            ("SentTimestamp", message.created.to_string()),
            ("ApproximateReceiveCount", message.receive_count.to_string()),
            (
                "ApproximateFirstReceiveTimestamp",
                message.first_receive.unwrap_or(0).to_string(),
            ),
            ("SenderId", self.config.account_id.clone()),
        ]);
        for (name, value) in [
            ("MessageGroupId", &message.group_id),
            ("MessageDeduplicationId", &message.deduplication_id),
            ("SequenceNumber", &message.sequence_number),
            ("AWSTraceHeader", &message.tracing_id),
        ] {
            if let Some(value) = value {
                system.insert(name, value.clone());
            }
        }
        if let Some(source) = &message.dead_letter_source_queue {
            system.insert("DeadLetterQueueSourceArn", self.config.arn(source));
        }
        if !system_names.iter().any(|n| n == "All") {
            system.retain(|key, _| system_names.iter().any(|n| n == key));
        }
        let attributes: Attributes = message
            .attributes
            .iter()
            .filter(|(key, _)| {
                attribute_names.iter().any(|name| {
                    name == "All"
                        || name == ".*"
                        || name == *key
                        || name
                            .strip_suffix(".*")
                            .is_some_and(|prefix| key.starts_with(prefix))
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
    let mut output = String::new();
    for (name, field) in [
        ("rusticmq_messages_visible", "Visible messages"),
        (
            "rusticmq_messages_invisible",
            "Messages awaiting visibility timeout",
        ),
        (
            "rusticmq_messages_delayed",
            "Messages awaiting their initial delivery",
        ),
    ] {
        let _ = writeln!(output, "# HELP {name} {field}\n# TYPE {name} gauge");
    }
    let time = now();
    for name in manager.list_queues(None) {
        if let Ok(stats) = manager.statistics(&name, time) {
            for (metric, count) in [
                ("visible", stats.visible),
                ("invisible", stats.invisible),
                ("delayed", stats.delayed),
            ] {
                let _ = writeln!(
                    output,
                    "rusticmq_messages_{metric}{{queue=\"{name}\"}} {count}"
                );
            }
        }
    }
    (
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        output,
    )
        .into_response()
}

async fn handle(
    State(mut service): State<SqsService>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if method == Method::OPTIONS {
        return cors((StatusCode::NO_CONTENT, "").into_response());
    }
    if service.config.wildcard_queue_urls {
        let protocol = uri.scheme_str().unwrap_or("http");
        let authority = uri
            .authority()
            .map(|value| value.as_str())
            .or_else(|| headers.get("host").and_then(|value| value.to_str().ok()))
            .unwrap_or("localhost");
        let postfix = if service.config.account_id.is_empty() {
            "queue"
        } else {
            &service.config.account_id
        };
        let path = uri.path().trim_end_matches('/');
        let suffix = format!("/{postfix}");
        let path = path.strip_suffix(&suffix).unwrap_or(path);
        service.config.base_url = format!("{protocol}://{authority}{path}");
    }
    let json_protocol = headers.contains_key("x-amz-target")
        || headers
            .get("content-type")
            .and_then(|h| h.to_str().ok())
            .is_some_and(|h| h.contains("json"));
    let parsed = if json_protocol {
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
    let request_id = uuid::Uuid::new_v4().to_string();
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
                value.to_string()
            } else {
                protocol::xml_response(&action, &value, &request_id)
            },
        ),
        Err(error) => (
            if error.code == "InternalError" {
                StatusCode::INTERNAL_SERVER_ERROR
            } else {
                StatusCode::BAD_REQUEST
            },
            if json_protocol {
                json!({"__type":error.json_type(),"Message":error.message}).to_string()
            } else {
                protocol::xml_error(&error, &request_id)
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
        "content-type",
        (if json_protocol {
            "application/x-amz-json-1.0"
        } else {
            "text/xml; charset=utf-8"
        })
        .parse()
        .unwrap(),
    );
    response
        .headers_mut()
        .insert("x-amzn-requestid", request_id.parse().unwrap());
    cors(response)
}

fn cors(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert("access-control-allow-origin", "*".parse().unwrap());
    response
        .headers_mut()
        .insert("access-control-allow-headers", "*".parse().unwrap());
    response.headers_mut().insert(
        "access-control-allow-methods",
        "GET, POST, OPTIONS".parse().unwrap(),
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
    let prefix = format!("{}/", context.trim_end_matches('/'));
    let path = path
        .path()
        .strip_prefix(&prefix)
        .ok_or_else(|| ApiError::invalid("QueueUrl must include the configured context path"))?;
    let parts: Vec<_> = path.trim_end_matches('/').split('/').collect();
    let name = match parts.as_slice() {
        [name] => *name,
        [account, name]
            if !account.is_empty() && account.bytes().all(|c| c.is_ascii_alphanumeric()) =>
        {
            *name
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
    } else if text.encode_utf16().count() > 1024 * 1024 {
        Some("MessageTooLong")
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
