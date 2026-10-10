use crate::model::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Deduplicated {
    message: Message,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReceiveAttempt {
    cached_at: Timestamp,
    message_ids: Vec<String>,
}

/// A reference identifies a specific queue incarnation, like an upstream actor
/// reference. Deleting and recreating a queue must not silently rebind routes.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueueRef {
    name: String,
    generation: Uuid,
}

#[derive(Debug, Clone)]
struct RunningMoveTask {
    data: MessageMoveTask,
    destination: QueueRef,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Queue {
    generation: Uuid,
    config: QueueConfig,
    copy_to: Option<QueueRef>,
    move_to: Option<QueueRef>,
    dead_letters: Option<QueueRef>,
    messages: BTreeMap<String, Message>,
    deduplication: BTreeMap<String, Deduplicated>,
    attempts: BTreeMap<String, ReceiveAttempt>,
    #[serde(skip)]
    moving: Option<RunningMoveTask>,
    #[serde(skip)]
    previous_moves: Vec<MessageMoveTask>,
}

impl Queue {
    fn selected(&self, count: usize, now: Timestamp) -> Vec<String> {
        let mut messages: Vec<_> = self.messages.values().collect();
        if !self.config.fifo {
            messages.sort_by_key(|m| (m.next_delivery, m.order));
            return messages
                .into_iter()
                .filter(|m| m.next_delivery <= now)
                .take(count)
                .map(|m| m.id.clone())
                .collect();
        }
        // Stay in one group while building a batch. An invisible or delayed head
        // blocks subsequent messages in its group, including visible successors.
        let mut groups: BTreeMap<&str, Vec<&Message>> = BTreeMap::new();
        for message in messages {
            groups
                .entry(message.group_id.as_deref().expect("FIFO group invariant"))
                .or_default()
                .push(message);
        }
        let mut selected = Vec::new();
        for group in groups.values_mut() {
            group.sort_by_key(|m| (m.created, m.order));
            for message in group {
                if selected.len() == count || message.next_delivery > now {
                    break;
                }
                selected.push(message.id.clone());
            }
            if selected.len() == count {
                break;
            }
        }
        selected
    }

    fn refresh_history(&mut self, message: &Message) {
        if let Some(entry) = message
            .deduplication_id
            .as_ref()
            .and_then(|id| self.deduplication.get_mut(id))
        {
            // Upstream history retains mutable message objects after deletion.
            if entry.message.id == message.id {
                entry.message = message.clone();
            }
        }
    }
}

/// Synchronous queue engine. Serialize access in the embedding application.
/// All times are epoch milliseconds; operations never block or sleep.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct QueueManager {
    #[serde(default)]
    relaxed_limits: bool,
    queues: BTreeMap<String, Queue>,
    next_order: u64,
    next_sequence: u64,
    #[serde(skip)]
    events: Vec<QueueEvent>,
    #[serde(skip)]
    move_tasks: BTreeMap<String, QueueRef>,
}

impl QueueManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Disable the optional SQS name-length limit, retaining name syntax checks.
    pub fn set_relaxed_limits(&mut self, relaxed: bool) {
        self.relaxed_limits = relaxed;
    }

    pub fn create_queue(
        &mut self,
        request: CreateQueue,
        now: Timestamp,
    ) -> Result<QueueConfig, Error> {
        if let Some(queue) = self.queues.get(&request.name) {
            return if request.matches(&queue.config) {
                Ok(queue.config.clone())
            } else {
                Err(Error::QueueAlreadyExists(request.name))
            };
        }
        validate_queue_name(&request.name, request.fifo, self.relaxed_limits)?;
        let config = request.into_config(now);
        let copy_to = config
            .copy_messages_to
            .as_deref()
            .and_then(|name| self.reference(name));
        let move_to = config
            .move_messages_to
            .as_deref()
            .and_then(|name| self.reference(name));
        let dead_letters = config
            .dead_letter_queue
            .as_ref()
            .and_then(|dlq| self.reference(&dlq.name));
        self.queues.insert(
            config.name.clone(),
            Queue {
                generation: Uuid::new_v4(),
                copy_to,
                move_to,
                dead_letters,
                config: config.clone(),
                messages: BTreeMap::new(),
                deduplication: BTreeMap::new(),
                attempts: BTreeMap::new(),
                moving: None,
                previous_moves: Vec::new(),
            },
        );
        self.events.push(QueueEvent::QueueCreated(config.clone()));
        Ok(config)
    }

    pub fn queue(&self, name: &str) -> Result<&QueueConfig, Error> {
        Ok(&self.get(name)?.config)
    }

    pub fn list_queues(&self, prefix: Option<&str>) -> Vec<String> {
        self.queues
            .keys()
            .filter(|name| prefix.is_none_or(|p| name.starts_with(p)))
            .cloned()
            .collect()
    }

    pub fn delete_queue(&mut self, name: &str) {
        self.queues.remove(name);
        self.move_tasks.retain(|_, source| source.name != name);
        self.events.push(QueueEvent::QueueDeleted(name.into()));
    }

    pub fn purge_queue(&mut self, name: &str) -> Result<(), Error> {
        let queue = self.get_mut(name)?;
        let ids = queue.messages.keys().cloned().collect::<Vec<_>>();
        queue.messages.clear();
        queue.deduplication.clear();
        self.events
            .extend(ids.into_iter().map(|id| QueueEvent::MessageRemoved {
                queue: name.into(),
                id,
            }));
        Ok(())
    }

    /// Replace mutable configuration; queue identity and FIFO mode are immutable.
    pub fn update_queue(&mut self, config: QueueConfig) -> Result<(), Error> {
        let dead_letters = config
            .dead_letter_queue
            .as_ref()
            .and_then(|dlq| self.reference(&dlq.name));
        let queue = self.get_mut(&config.name)?;
        if config.fifo != queue.config.fifo {
            return Err(Error::InvalidParameter(
                "FifoQueue cannot be changed".into(),
            ));
        }
        if config.copy_messages_to != queue.config.copy_messages_to
            || config.move_messages_to != queue.config.move_messages_to
        {
            return Err(Error::InvalidParameter(
                "Copy/move routes cannot be changed after creation".into(),
            ));
        }
        queue.dead_letters = dead_letters;
        queue.config = config.clone();
        self.events.push(QueueEvent::QueueMetadataUpdated(config));
        Ok(())
    }

    pub fn tag_queue(&mut self, name: &str, tags: BTreeMap<String, String>) -> Result<(), Error> {
        let queue = self.get_mut(name)?;
        queue.config.tags.extend(tags);
        let config = queue.config.clone();
        self.events.push(QueueEvent::QueueMetadataUpdated(config));
        Ok(())
    }

    pub fn untag_queue(&mut self, name: &str, keys: &[String]) -> Result<(), Error> {
        let queue = self.get_mut(name)?;
        for key in keys {
            queue.config.tags.remove(key);
        }
        let config = queue.config.clone();
        self.events.push(QueueEvent::QueueMetadataUpdated(config));
        Ok(())
    }

    pub fn dead_letter_sources(&self, name: &str) -> Vec<String> {
        self.queues
            .iter()
            .filter(|(_, q)| {
                q.config
                    .dead_letter_queue
                    .as_ref()
                    .is_some_and(|dlq| dlq.name == name)
            })
            .map(|(name, _)| name.clone())
            .collect()
    }

    pub fn send_message(
        &mut self,
        name: &str,
        request: SendMessage,
        now: Timestamp,
    ) -> Result<Message, Error> {
        // Validate the entire routing graph before committing copies, preventing
        // a misconfigured cycle from causing recursion or partially writing.
        self.validate_routing(name, &mut BTreeSet::new())?;
        self.send_routed(name, request, now)
    }

    fn validate_routing(&self, name: &str, path: &mut BTreeSet<String>) -> Result<(), Error> {
        if !path.insert(name.into()) {
            return Err(Error::RoutingCycle(name.into()));
        }
        let queue = self.get(name)?;
        for next in [&queue.copy_to, &queue.move_to].into_iter().flatten() {
            if self.is_live(next) {
                self.validate_routing(&next.name, path)?;
            }
        }
        path.remove(name);
        Ok(())
    }

    fn send_routed(
        &mut self,
        name: &str,
        mut request: SendMessage,
        now: Timestamp,
    ) -> Result<Message, Error> {
        let config = self.queue(name)?.clone();
        let copy_to = self.get(name)?.copy_to.clone();
        let move_to = self.get(name)?.move_to.clone();
        self.prepare_fifo(&config, &mut request)?;
        if let Some(next) = copy_to
            && self.is_live(&next)
        {
            self.send_routed(&next.name, request.clone(), now)?;
        }
        if let Some(next) = move_to {
            // Sending to an upstream stopped actor cannot receive a response.
            // Expose the unavailable destination instead of rebinding or storing
            // on the source, which would change the configured routing behavior.
            if !self.is_live(&next) {
                return Err(Error::QueueDoesNotExist(next.name));
            }
            return self.send_routed(&next.name, request, now);
        }
        let queue = self.get_mut(name)?;
        queue
            .deduplication
            .retain(|_, entry| after(entry.message.created, 300_000) >= now);
        if config.fifo
            && let Some(previous) = request
                .deduplication_id
                .as_ref()
                .and_then(|id| queue.deduplication.get(id))
        {
            return Ok(previous.message.clone());
        }
        let message = Message {
            id: request.id.unwrap_or_else(|| Uuid::new_v4().to_string()),
            receipt_handle: None,
            body: request.body,
            attributes: request.attributes,
            next_delivery: request
                .delivery_at
                .unwrap_or_else(|| after(now, request.delay_ms.unwrap_or(config.delay_ms))),
            created: now,
            first_receive: None,
            receive_count: 0,
            group_id: request.group_id,
            deduplication_id: request.deduplication_id,
            tracing_id: request.tracing_id,
            sequence_number: request.sequence_number,
            dead_letter_source_queue: request.dead_letter_source_queue,
            order: self.next_order,
        };
        self.next_order += 1;
        self.insert_message(name, message.clone())?;
        Ok(message)
    }

    fn prepare_fifo(
        &mut self,
        config: &QueueConfig,
        request: &mut SendMessage,
    ) -> Result<(), Error> {
        if config.fifo {
            if request.group_id.as_ref().is_none_or(String::is_empty) {
                return Err(Error::InvalidParameter(
                    "MessageGroupId is required for FIFO queues".into(),
                ));
            }
            if request.deduplication_id.is_none() && config.content_based_deduplication {
                request.deduplication_id =
                    Some(format!("{:x}", Sha256::digest(request.body.as_bytes())));
            }
            if request
                .deduplication_id
                .as_ref()
                .is_none_or(String::is_empty)
            {
                return Err(Error::InvalidParameter(
                    "MessageDeduplicationId is required for FIFO queues".into(),
                ));
            }
            if request.sequence_number.is_none() {
                request.sequence_number = Some(self.next_sequence.to_string());
                self.next_sequence += 1;
            }
        }
        Ok(())
    }

    fn insert_message(&mut self, name: &str, message: Message) -> Result<(), Error> {
        let queue = self.get_mut(name)?;
        if let Some(id) = &message.deduplication_id {
            queue
                .deduplication
                .entry(id.clone())
                .or_insert_with(|| Deduplicated {
                    message: message.clone(),
                });
        }
        queue.messages.insert(message.id.clone(), message.clone());
        self.events.push(QueueEvent::MessageAdded {
            queue: name.into(),
            message,
        });
        Ok(())
    }

    pub fn receive_messages(
        &mut self,
        name: &str,
        request: ReceiveMessages,
        now: Timestamp,
    ) -> Result<Vec<Message>, Error> {
        let queue = self.get_mut(name)?;
        let cached = request
            .attempt_id
            .as_ref()
            .and_then(|id| queue.attempts.get(id))
            .cloned();
        let (ids, from_cache) = if let Some(attempt) = cached {
            if after(attempt.cached_at, 300_000) < now {
                queue.attempts.remove(request.attempt_id.as_ref().unwrap());
                return Err(Error::AttemptExpired);
            }
            if attempt
                .message_ids
                .iter()
                .any(|id| !queue.messages.contains_key(id))
            {
                return Err(Error::AttemptInvalid);
            }
            if attempt.message_ids.is_empty() {
                (queue.selected(request.count, now), false)
            } else {
                (attempt.message_ids, true)
            }
        } else {
            (queue.selected(request.count, now), false)
        };

        let config = queue.config.clone();
        let dead_letters = queue.dead_letters.clone();
        let mut result = Vec::new();
        for id in ids {
            let message = self
                .get(name)?
                .messages
                .get(&id)
                .expect("selection invariant")
                .clone();
            if !from_cache
                && config
                    .dead_letter_queue
                    .as_ref()
                    .is_some_and(|dlq| message.receive_count >= dlq.max_receive_count)
            {
                if let Some(target) = &dead_letters
                    && self.is_live(target)
                {
                    self.move_to_dlq(&target.name, name, message.clone(), now)?;
                }
                self.get_mut(name)?.messages.remove(&id);
                self.events.push(QueueEvent::MessageRemoved {
                    queue: name.into(),
                    id,
                });
                continue;
            }
            let queue = self.get_mut(name)?;
            let message = queue.messages.get_mut(&id).expect("selection invariant");
            message.receipt_handle = Some(format!("{}#{}", message.id, Uuid::new_v4()));
            message.next_delivery = after(
                now,
                request
                    .visibility_timeout_ms
                    .unwrap_or(config.visibility_timeout_ms),
            );
            message.receive_count = message.receive_count.saturating_add(1);
            message.first_receive.get_or_insert(now);
            let message = message.clone();
            queue.refresh_history(&message);
            self.events.push(QueueEvent::MessageUpdated {
                queue: name.into(),
                message: message.clone(),
            });
            result.push(message);
        }
        if let Some(id) = request.attempt_id {
            let queue = self.get_mut(name)?;
            queue.attempts.insert(
                id,
                ReceiveAttempt {
                    cached_at: now,
                    message_ids: result.iter().map(|m| m.id.clone()).collect(),
                },
            );
            queue
                .attempts
                .retain(|_, attempt| after(attempt.cached_at, 900_000) >= now);
        }
        Ok(result)
    }

    fn move_to_dlq(
        &mut self,
        destination: &str,
        source: &str,
        mut message: Message,
        now: Timestamp,
    ) -> Result<(), Error> {
        message.dead_letter_source_queue = Some(source.into());
        // Upstream copies incoming DLQ moves before destination deduplication;
        // the copy is a new send, retaining the original absolute delivery time.
        if let Some(target) = self.get(destination)?.copy_to.clone()
            && self.is_live(&target)
        {
            let copied = SendMessage {
                id: Some(message.id.clone()),
                body: message.body.clone(),
                attributes: message.attributes.clone(),
                delay_ms: None,
                delivery_at: Some(message.next_delivery),
                group_id: message.group_id.clone(),
                deduplication_id: message.deduplication_id.clone(),
                tracing_id: message.tracing_id.clone(),
                sequence_number: message.sequence_number.clone(),
                dead_letter_source_queue: message.dead_letter_source_queue.clone(),
            };
            self.send_message(&target.name, copied, now)?;
        }
        let queue = self.get_mut(destination)?;
        queue
            .deduplication
            .retain(|_, entry| after(entry.message.created, 300_000) >= now);
        if queue.config.fifo {
            if message
                .deduplication_id
                .as_ref()
                .is_some_and(|id| queue.deduplication.contains_key(id))
            {
                return Ok(());
            }
            if message.group_id.is_none() {
                return Err(Error::InvalidParameter(
                    "Cannot move a message without a group to a FIFO queue".into(),
                ));
            }
            message.deduplication_id = Some(message.id.clone());
        }
        self.insert_message(destination, message)
    }

    pub fn delete_message(&mut self, name: &str, receipt: &str) -> Result<(), Error> {
        let id = receipt.split('#').next().unwrap_or("");
        let queue = self.get_mut(name)?;
        let message = queue
            .messages
            .get(id)
            .filter(|m| m.receipt_handle.as_deref() == Some(receipt))
            .ok_or_else(|| Error::InvalidReceiptHandle(receipt.into()))?;
        let message = message.clone();
        queue.refresh_history(&message);
        queue.messages.remove(id);
        self.events.push(QueueEvent::MessageRemoved {
            queue: name.into(),
            id: id.into(),
        });
        Ok(())
    }

    pub fn change_visibility(
        &mut self,
        name: &str,
        receipt: &str,
        timeout_ms: u64,
        now: Timestamp,
    ) -> Result<(), Error> {
        let id = receipt.split('#').next().unwrap_or("");
        let queue = self.get_mut(name)?;
        let message = queue
            .messages
            .get_mut(id)
            .filter(|m| m.receipt_handle.as_deref() == Some(receipt))
            .ok_or_else(|| Error::InvalidReceiptHandle(receipt.into()))?;
        message.next_delivery = after(now, timeout_ms);
        let message = message.clone();
        queue.refresh_history(&message);
        self.events.push(QueueEvent::MessageUpdated {
            queue: name.into(),
            message,
        });
        Ok(())
    }

    pub fn statistics(&self, name: &str, now: Timestamp) -> Result<QueueStatistics, Error> {
        let mut stats = QueueStatistics::default();
        for message in self.get(name)?.messages.values() {
            // Match upstream GetQueueStatistics's strict comparison at the boundary.
            if message.next_delivery < now {
                stats.visible += 1;
            } else if message.receipt_handle.is_some() {
                stats.invisible += 1;
            } else {
                stats.delayed += 1;
            }
        }
        Ok(stats)
    }

    pub fn messages(&self, name: &str) -> Result<Vec<Message>, Error> {
        Ok(self.get(name)?.messages.values().cloned().collect())
    }

    pub fn start_move_task(
        &mut self,
        request: StartMoveTask,
        now: Timestamp,
    ) -> Result<String, Error> {
        let source = self.get(&request.source)?;
        if source.moving.is_some() {
            return Err(Error::MoveTaskAlreadyRunning(request.source));
        }
        if request
            .max_messages_per_second
            .is_some_and(|rate| rate == 0 || rate > 500)
        {
            return Err(Error::InvalidParameter(
                "MaxNumberOfMessagesPerSecond must be between 1 and 500".into(),
            ));
        }
        let destination = request
            .destination
            .or_else(|| self.dead_letter_sources(&request.source).into_iter().next())
            .ok_or_else(|| {
                Error::InvalidParameter("No source queue references this dead-letter queue".into())
            })?;
        if destination == request.source {
            return Err(Error::InvalidParameter(
                "Source and destination must differ".into(),
            ));
        }
        let destination = self
            .reference(&destination)
            .ok_or_else(|| Error::QueueDoesNotExist(destination.clone()))?;
        let handle = Uuid::new_v4().to_string();
        let data = MessageMoveTask {
            handle: handle.clone(),
            moved: 0,
            to_move: source.messages.len() as u64,
            source_arn: request.source_arn,
            destination_arn: request.destination_arn,
            max_messages_per_second: request.max_messages_per_second,
            started: now,
            status: MoveTaskStatus::Running,
            failure_reason: None,
        };
        self.get_mut(&request.source)?.moving = Some(RunningMoveTask { data, destination });
        self.move_tasks.insert(
            handle.clone(),
            self.reference(&request.source).expect("source invariant"),
        );
        Ok(handle)
    }

    /// Move one message, including delayed or invisible messages. The caller
    /// schedules subsequent steps using the returned interval. No interval means
    /// the task has completed or was cancelled. Each step is serialized with
    /// other queue operations; rate-limited tasks do not hold a manager lock
    /// between steps.
    pub fn step_move_task(
        &mut self,
        handle: &str,
        now: Timestamp,
    ) -> Result<Option<std::time::Duration>, Error> {
        let Some(source) = self.move_tasks.get(handle).cloned() else {
            return Ok(None);
        };
        if !self.is_live(&source) {
            self.move_tasks.remove(handle);
            return Ok(None);
        }
        let running = self
            .get(&source.name)?
            .moving
            .clone()
            .expect("task index invariant");
        let queue = self.get(&source.name)?;
        let message = if queue.config.fifo {
            queue.messages.values().min_by_key(|m| (m.created, m.order))
        } else {
            queue
                .messages
                .values()
                .min_by_key(|m| (m.next_delivery, m.order))
        }
        .cloned();
        let Some(message) = message else {
            self.finish_move_task(&source.name, MoveTaskStatus::Completed, None)?;
            return Ok(None);
        };
        // Upstream sends asynchronously without awaiting destination acceptance.
        // Preserve its source removal/counting even if the bound actor is gone
        // or the destination rejects the message; do not rebind to a new queue.
        if self.is_live(&running.destination) {
            let _ = self.send_message(
                &running.destination.name,
                SendMessage {
                    id: Some(message.id.clone()),
                    body: message.body.clone(),
                    attributes: message.attributes.clone(),
                    delivery_at: Some(message.next_delivery),
                    group_id: message.group_id.clone(),
                    deduplication_id: message.deduplication_id.clone(),
                    tracing_id: message.tracing_id.clone(),
                    sequence_number: message.sequence_number.clone(),
                    dead_letter_source_queue: message.dead_letter_source_queue.clone(),
                    ..Default::default()
                },
                now,
            );
        }
        let queue = self.get_mut(&source.name)?;
        queue.messages.remove(&message.id);
        queue
            .moving
            .as_mut()
            .expect("task index invariant")
            .data
            .moved += 1;
        self.events.push(QueueEvent::MessageRemoved {
            queue: source.name,
            id: message.id,
        });
        Ok(Some(
            running
                .data
                .max_messages_per_second
                .map(|rate| std::time::Duration::from_nanos(1_000_000_000 / rate as u64))
                .unwrap_or_default(),
        ))
    }

    fn finish_move_task(
        &mut self,
        source: &str,
        status: MoveTaskStatus,
        failure: Option<String>,
    ) -> Result<(), Error> {
        let queue = self.get_mut(source)?;
        let mut running = queue.moving.take().expect("task index invariant");
        running.data.status = status;
        running.data.failure_reason = failure;
        running.data.to_move = queue.messages.len() as u64;
        let handle = running.data.handle.clone();
        queue.previous_moves.push(running.data);
        self.move_tasks.remove(&handle);
        Ok(())
    }

    pub fn cancel_move_task(&mut self, handle: &str) -> Result<u64, Error> {
        let source = self
            .move_tasks
            .remove(handle)
            .ok_or_else(|| Error::InvalidMoveTaskHandle(handle.into()))?;
        let running = self
            .get_mut(&source.name)?
            .moving
            .take()
            .ok_or_else(|| Error::InvalidMoveTaskHandle(handle.into()))?;
        // Match upstream: cancelled tasks are removed rather than appended to
        // the completed task history.
        Ok(running.data.moved)
    }

    pub fn list_move_tasks(&self, source: &str) -> Result<Vec<MessageMoveTask>, Error> {
        let queue = self.get(source)?;
        let mut tasks: Vec<_> = queue.moving.iter().map(|task| task.data.clone()).collect();
        tasks.extend(queue.previous_moves.iter().rev().cloned());
        Ok(tasks)
    }

    /// Drain the ordered mutation events for a persistence or metrics listener.
    pub fn drain_events(&mut self) -> Vec<QueueEvent> {
        std::mem::take(&mut self.events)
    }

    /// Return failed persistence events to the front of the pending stream.
    pub fn requeue_events(&mut self, mut events: Vec<QueueEvent>) {
        events.append(&mut self.events);
        self.events = events;
    }

    /// Restore definitions in dependency order so bound routes are available
    /// before their source queues are created. Missing external references are
    /// left unbound, matching upstream; dependency cycles fail before mutation.
    pub fn restore_queues(&mut self, configs: Vec<QueueConfig>) -> Result<(), Error> {
        let mut remaining: BTreeMap<_, _> = configs
            .into_iter()
            .map(|config| (config.name.clone(), config))
            .collect();
        let mut sorted = Vec::new();
        while !remaining.is_empty() {
            let ready: Vec<_> = remaining
                .iter()
                .filter(|(_, config)| {
                    [&config.copy_messages_to, &config.move_messages_to]
                        .into_iter()
                        .flatten()
                        .chain(config.dead_letter_queue.iter().map(|dlq| &dlq.name))
                        .all(|name| !remaining.contains_key(name))
                })
                .map(|(name, _)| name.clone())
                .collect();
            if ready.is_empty() {
                return Err(Error::InvalidParameter(
                    "Queue definitions contain a routing cycle".into(),
                ));
            }
            for name in ready {
                sorted.push(remaining.remove(&name).expect("definition invariant"));
            }
        }
        for config in sorted {
            let event_count = self.events.len();
            self.create_queue(
                CreateQueue {
                    name: config.name.clone(),
                    visibility_timeout_ms: Some(config.visibility_timeout_ms),
                    delay_ms: Some(config.delay_ms),
                    receive_wait_ms: Some(config.receive_wait_ms),
                    dead_letter_queue: config.dead_letter_queue.clone(),
                    fifo: config.fifo,
                    content_based_deduplication: config.content_based_deduplication,
                    copy_messages_to: config.copy_messages_to.clone(),
                    move_messages_to: config.move_messages_to.clone(),
                    tags: config.tags.clone(),
                },
                config.created,
            )?;
            self.get_mut(&config.name)?.config = config.clone();
            if self.events.len() > event_count
                && let Some(QueueEvent::QueueCreated(created)) = self.events.last_mut()
            {
                *created = config;
            }
        }
        Ok(())
    }

    pub fn restore_messages(&mut self, queue: &str, messages: Vec<Message>) -> Result<(), Error> {
        let fifo = self.queue(queue)?.fifo;
        if fifo && messages.iter().any(|message| message.group_id.is_none()) {
            return Err(Error::InvalidParameter(
                "Restored FIFO messages must have group IDs".into(),
            ));
        }
        for message in messages {
            self.next_order = self.next_order.max(message.order.saturating_add(1));
            if let Some(number) = message
                .sequence_number
                .as_ref()
                .and_then(|number| number.parse::<u64>().ok())
            {
                self.next_sequence = self.next_sequence.max(number.saturating_add(1));
            }
            self.insert_message(queue, message)?;
        }
        Ok(())
    }

    fn reference(&self, name: &str) -> Option<QueueRef> {
        self.queues.get(name).map(|queue| QueueRef {
            name: name.into(),
            generation: queue.generation,
        })
    }

    fn is_live(&self, reference: &QueueRef) -> bool {
        self.queues
            .get(&reference.name)
            .is_some_and(|queue| queue.generation == reference.generation)
    }

    fn get(&self, name: &str) -> Result<&Queue, Error> {
        self.queues
            .get(name)
            .ok_or_else(|| Error::QueueDoesNotExist(name.into()))
    }

    fn get_mut(&mut self, name: &str) -> Result<&mut Queue, Error> {
        self.queues
            .get_mut(name)
            .ok_or_else(|| Error::QueueDoesNotExist(name.into()))
    }
}

fn validate_queue_name(name: &str, fifo: bool, relaxed: bool) -> Result<(), Error> {
    let base = if fifo {
        name.strip_suffix(".fifo").unwrap_or("")
    } else {
        name
    };
    if base.is_empty()
        || (!relaxed && name.len() > 80)
        || !base
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(Error::InvalidParameter("Invalid queue name".into()));
    }
    Ok(())
}
