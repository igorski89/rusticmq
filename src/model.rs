use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type Timestamp = i64;
pub type Attributes = BTreeMap<String, MessageAttribute>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttributeValue {
    String(String),
    Number(String),
    Binary(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageAttribute {
    pub value: AttributeValue,
    pub custom_type: Option<String>,
}

impl MessageAttribute {
    pub fn data_type(&self) -> String {
        let base = match self.value {
            AttributeValue::String(_) => "String",
            AttributeValue::Number(_) => "Number",
            AttributeValue::Binary(_) => "Binary",
        };
        match &self.custom_type {
            Some(custom) => format!("{base}.{custom}"),
            None => base.to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadLetterQueue {
    pub name: String,
    pub max_receive_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueConfig {
    pub name: String,
    pub visibility_timeout_ms: u64,
    pub delay_ms: u64,
    pub receive_wait_ms: u64,
    pub created: Timestamp,
    pub last_modified: Timestamp,
    pub dead_letter_queue: Option<DeadLetterQueue>,
    pub fifo: bool,
    pub content_based_deduplication: bool,
    pub copy_messages_to: Option<String>,
    pub move_messages_to: Option<String>,
    pub tags: BTreeMap<String, String>,
}

/// Optional fields retain the distinction between an omitted creation attribute
/// and an explicitly supplied default, required for idempotent CreateQueue.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CreateQueue {
    pub name: String,
    pub visibility_timeout_ms: Option<u64>,
    pub delay_ms: Option<u64>,
    pub receive_wait_ms: Option<u64>,
    pub dead_letter_queue: Option<DeadLetterQueue>,
    pub fifo: bool,
    pub content_based_deduplication: bool,
    pub copy_messages_to: Option<String>,
    pub move_messages_to: Option<String>,
    pub tags: BTreeMap<String, String>,
}

impl CreateQueue {
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Self::default()
        }
    }

    pub(crate) fn matches(&self, existing: &QueueConfig) -> bool {
        self.visibility_timeout_ms
            .is_none_or(|v| v == existing.visibility_timeout_ms)
            && self.delay_ms.is_none_or(|v| v == existing.delay_ms)
            && self
                .receive_wait_ms
                .is_none_or(|v| v == existing.receive_wait_ms)
            && (self.dead_letter_queue.is_none()
                || self.dead_letter_queue == existing.dead_letter_queue)
            && self.fifo == existing.fifo
            && self.content_based_deduplication == existing.content_based_deduplication
            && (self.copy_messages_to.is_none()
                || self.copy_messages_to == existing.copy_messages_to)
            && (self.move_messages_to.is_none()
                || self.move_messages_to == existing.move_messages_to)
            && (self.tags.is_empty() || self.tags == existing.tags)
    }

    pub(crate) fn into_config(self, now: Timestamp) -> QueueConfig {
        QueueConfig {
            name: self.name,
            visibility_timeout_ms: self.visibility_timeout_ms.unwrap_or(30_000),
            delay_ms: self.delay_ms.unwrap_or(0),
            receive_wait_ms: self.receive_wait_ms.unwrap_or(0),
            created: now,
            last_modified: now,
            dead_letter_queue: self.dead_letter_queue,
            fifo: self.fifo,
            content_based_deduplication: self.content_based_deduplication,
            copy_messages_to: self.copy_messages_to,
            move_messages_to: self.move_messages_to,
            tags: self.tags,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SendMessage {
    pub id: Option<String>,
    pub body: String,
    pub attributes: Attributes,
    pub delay_ms: Option<u64>,
    /// Absolute delivery time for embedded sends and persistence replay.
    pub delivery_at: Option<Timestamp>,
    pub group_id: Option<String>,
    pub deduplication_id: Option<String>,
    pub tracing_id: Option<String>,
    pub sequence_number: Option<String>,
    pub dead_letter_source_queue: Option<String>,
}

impl SendMessage {
    pub fn body(body: impl Into<String>) -> Self {
        Self {
            body: body.into(),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub id: String,
    pub receipt_handle: Option<String>,
    pub body: String,
    pub attributes: Attributes,
    pub next_delivery: Timestamp,
    pub created: Timestamp,
    pub first_receive: Option<Timestamp>,
    pub receive_count: u32,
    pub group_id: Option<String>,
    pub deduplication_id: Option<String>,
    pub tracing_id: Option<String>,
    pub sequence_number: Option<String>,
    pub dead_letter_source_queue: Option<String>,
    pub(crate) order: u64,
}

#[derive(Debug, Clone, Default)]
pub struct ReceiveMessages {
    pub count: usize,
    pub visibility_timeout_ms: Option<u64>,
    pub attempt_id: Option<String>,
}

impl ReceiveMessages {
    pub fn count(count: usize) -> Self {
        Self {
            count,
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueStatistics {
    pub visible: usize,
    pub invisible: usize,
    pub delayed: usize,
}

/// Per-queue traffic since the process started or the queue was created.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueueActivity {
    /// Messages stored by sends, including copy/move routing into this queue;
    /// dead-letter and message-move-task arrivals are not sends.
    pub sent: u64,
    /// FIFO sends accepted without storing, as duplicates within five minutes.
    pub deduplicated: u64,
    pub received: u64,
    pub deleted: u64,
    /// Last send addressed to this queue, whether stored or deduplicated.
    pub last_send: Option<Timestamp>,
    /// Last receive request, including empty ones and long-poll wakeups.
    pub last_receive: Option<Timestamp>,
}

#[derive(Debug, Clone)]
pub struct StartMoveTask {
    pub source: String,
    pub destination: Option<String>,
    pub source_arn: String,
    pub destination_arn: Option<String>,
    pub max_messages_per_second: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum MoveTaskStatus {
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageMoveTask {
    pub handle: String,
    pub moved: u64,
    pub to_move: u64,
    pub source_arn: String,
    pub destination_arn: Option<String>,
    pub max_messages_per_second: Option<u32>,
    pub started: Timestamp,
    pub status: MoveTaskStatus,
    pub failure_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueueEvent {
    QueueCreated(QueueConfig),
    QueueDeleted(String),
    QueueMetadataUpdated(QueueConfig),
    MessageAdded { queue: String, message: Message },
    MessageUpdated { queue: String, message: Message },
    MessageRemoved { queue: String, id: String },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("The specified queue does not exist: {0}")]
    QueueDoesNotExist(String),
    #[error("A queue already exists with the same name and different attributes: {0}")]
    QueueAlreadyExists(String),
    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),
    #[error("The receipt handle is invalid: {0}")]
    InvalidReceiptHandle(String),
    #[error("Receive request attempt expired")]
    AttemptExpired,
    #[error("Receive request attempt invalid")]
    AttemptInvalid,
    #[error("Queue routing cycle: {0}")]
    RoutingCycle(String),
    #[error("A message move task is already running on queue \"{0}\"")]
    MoveTaskAlreadyRunning(String),
    #[error("The task handle \"{0}\" is not valid or does not exist")]
    InvalidMoveTaskHandle(String),
}

pub(crate) fn after(now: Timestamp, delta_ms: u64) -> Timestamp {
    now.saturating_add(i64::try_from(delta_ms).unwrap_or(i64::MAX))
}
