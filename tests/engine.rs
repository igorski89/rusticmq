use rusticmq::*;
use std::collections::BTreeMap;

fn manager() -> QueueManager {
    let mut manager = QueueManager::new();
    manager.create_queue(CreateQueue::named("q"), 0).unwrap();
    manager
}

fn fifo(manager: &mut QueueManager, name: &str) {
    manager
        .create_queue(
            CreateQueue {
                fifo: true,
                content_based_deduplication: true,
                ..CreateQueue::named(name)
            },
            0,
        )
        .unwrap();
}

fn grouped(body: &str, group: &str) -> SendMessage {
    SendMessage {
        group_id: Some(group.into()),
        ..SendMessage::body(body)
    }
}

fn receive(manager: &mut QueueManager, queue: &str, count: usize, now: i64) -> Vec<Message> {
    manager
        .receive_messages(queue, ReceiveMessages::count(count), now)
        .unwrap()
}

#[test]
fn create_is_idempotent_only_for_matching_supplied_attributes() {
    let mut manager = QueueManager::new();
    let config = manager
        .create_queue(
            CreateQueue {
                visibility_timeout_ms: Some(500),
                ..CreateQueue::named("q")
            },
            10,
        )
        .unwrap();
    assert_eq!(
        manager.create_queue(CreateQueue::named("q"), 20).unwrap(),
        config
    );
    assert!(matches!(
        manager.create_queue(
            CreateQueue {
                visibility_timeout_ms: Some(30_000),
                ..CreateQueue::named("q")
            },
            20
        ),
        Err(Error::QueueAlreadyExists(_))
    ));
}

#[test]
fn queue_names_and_fifo_suffix_are_checked() {
    let mut manager = QueueManager::new();
    for name in ["", "bad.name", "space name"] {
        assert!(manager.create_queue(CreateQueue::named(name), 0).is_err());
    }
    assert!(
        manager
            .create_queue(
                CreateQueue {
                    fifo: true,
                    ..CreateQueue::named("q")
                },
                0
            )
            .is_err()
    );
    fifo(&mut manager, "q.fifo");
}

#[test]
fn delays_visibility_and_statistics_match_upstream_boundaries() {
    let mut manager = manager();
    manager
        .send_message(
            "q",
            SendMessage {
                delay_ms: Some(100),
                ..SendMessage::body("delayed")
            },
            0,
        )
        .unwrap();
    assert_eq!(manager.statistics("q", 99).unwrap().delayed, 1);
    assert!(receive(&mut manager, "q", 10, 99).is_empty());
    // Upstream statistics uses '<', while receiving uses '<='.
    assert_eq!(manager.statistics("q", 100).unwrap().delayed, 1);
    let first = receive(&mut manager, "q", 10, 100).remove(0);
    assert_eq!(first.next_delivery, 30_100);
    assert_eq!(first.first_receive, Some(100));
    assert_eq!(first.receive_count, 1);
    assert_eq!(manager.statistics("q", 101).unwrap().invisible, 1);
    assert!(receive(&mut manager, "q", 10, 30_099).is_empty());
    let second = receive(&mut manager, "q", 10, 30_100).remove(0);
    assert_eq!(first.id, second.id);
    assert_ne!(first.receipt_handle, second.receipt_handle);
    assert_eq!(second.first_receive, Some(100));
    assert_eq!(second.receive_count, 2);
}

#[test]
fn stale_receipts_cannot_delete_or_change_visibility() {
    let mut manager = manager();
    manager
        .send_message("q", SendMessage::body("hello"), 0)
        .unwrap();
    let first = receive(&mut manager, "q", 1, 0).remove(0);
    let second = receive(&mut manager, "q", 1, 30_000).remove(0);
    let stale = first.receipt_handle.unwrap();
    assert!(matches!(
        manager.delete_message("q", &stale),
        Err(Error::InvalidReceiptHandle(_))
    ));
    assert!(matches!(
        manager.change_visibility("q", &stale, 0, 30_000),
        Err(Error::InvalidReceiptHandle(_))
    ));
    manager
        .delete_message("q", &second.receipt_handle.unwrap())
        .unwrap();
    assert!(manager.messages("q").unwrap().is_empty());
}

#[test]
fn decreasing_visibility_reorders_standard_delivery() {
    let mut manager = manager();
    manager
        .send_message("q", SendMessage::body("first"), 0)
        .unwrap();
    let first = receive(&mut manager, "q", 1, 0).remove(0);
    manager
        .send_message(
            "q",
            SendMessage {
                delay_ms: Some(200),
                ..SendMessage::body("second")
            },
            0,
        )
        .unwrap();
    manager
        .change_visibility("q", &first.receipt_handle.unwrap(), 100, 0)
        .unwrap();
    assert_eq!(receive(&mut manager, "q", 1, 100)[0].body, "first");
    assert_eq!(receive(&mut manager, "q", 1, 200)[0].body, "second");
}

#[test]
fn receive_zero_visibility_never_duplicates_a_message_within_a_batch() {
    let mut manager = manager();
    manager
        .send_message("q", SendMessage::body("hello"), 0)
        .unwrap();
    let request = ReceiveMessages {
        count: 10,
        visibility_timeout_ms: Some(0),
        attempt_id: None,
    };
    assert_eq!(
        manager
            .receive_messages("q", request.clone(), 0)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(manager.receive_messages("q", request, 0).unwrap().len(), 1);
    assert!(receive(&mut manager, "q", 0, 0).is_empty());
}

#[test]
fn fifo_blocks_a_group_while_other_groups_remain_available() {
    let mut manager = QueueManager::new();
    fifo(&mut manager, "q.fifo");
    for (body, group) in [("a1", "a"), ("a2", "a"), ("b1", "b")] {
        manager
            .send_message("q.fifo", grouped(body, group), 0)
            .unwrap();
    }
    let first = receive(&mut manager, "q.fifo", 1, 0).remove(0);
    assert_eq!(first.body, "a1");
    assert_eq!(receive(&mut manager, "q.fifo", 10, 1)[0].body, "b1");
    manager
        .delete_message("q.fifo", &first.receipt_handle.unwrap())
        .unwrap();
    assert_eq!(receive(&mut manager, "q.fifo", 1, 2)[0].body, "a2");
}

#[test]
fn fifo_batches_keep_group_order_even_at_equal_timestamps() {
    let mut manager = QueueManager::new();
    fifo(&mut manager, "q.fifo");
    for body in ["one", "two", "three"] {
        manager
            .send_message("q.fifo", grouped(body, "g"), 0)
            .unwrap();
    }
    let batch = receive(&mut manager, "q.fifo", 10, 0);
    assert_eq!(
        batch.iter().map(|m| m.body.as_str()).collect::<Vec<_>>(),
        ["one", "two", "three"]
    );
    assert!(
        batch
            .windows(2)
            .all(|pair| pair[0].sequence_number != pair[1].sequence_number)
    );
}

#[test]
fn a_delayed_fifo_head_blocks_its_visible_successor() {
    let mut manager = QueueManager::new();
    fifo(&mut manager, "q.fifo");
    manager
        .send_message(
            "q.fifo",
            SendMessage {
                delay_ms: Some(100),
                ..grouped("head", "g")
            },
            0,
        )
        .unwrap();
    manager
        .send_message("q.fifo", grouped("tail", "g"), 0)
        .unwrap();
    assert!(receive(&mut manager, "q.fifo", 10, 99).is_empty());
    assert_eq!(receive(&mut manager, "q.fifo", 10, 100).len(), 2);
}

#[test]
fn deduplication_survives_delete_and_expires_after_five_minutes() {
    let mut manager = QueueManager::new();
    fifo(&mut manager, "q.fifo");
    let sent = manager
        .send_message("q.fifo", grouped("hello", "g"), 0)
        .unwrap();
    let received = receive(&mut manager, "q.fifo", 1, 0).remove(0);
    manager
        .delete_message("q.fifo", &received.receipt_handle.clone().unwrap())
        .unwrap();
    let duplicate = manager
        .send_message("q.fifo", grouped("hello", "different-group"), 300_000)
        .unwrap();
    assert_eq!(duplicate, received);
    assert!(manager.messages("q.fifo").unwrap().is_empty());
    let fresh = manager
        .send_message("q.fifo", grouped("hello", "g"), 300_001)
        .unwrap();
    assert_ne!(fresh.id, sent.id);
}

#[test]
fn purge_clears_fifo_deduplication() {
    let mut manager = QueueManager::new();
    fifo(&mut manager, "q.fifo");
    let first = manager
        .send_message("q.fifo", grouped("hello", "g"), 0)
        .unwrap();
    manager.purge_queue("q.fifo").unwrap();
    let second = manager
        .send_message("q.fifo", grouped("hello", "g"), 1)
        .unwrap();
    assert_ne!(first.id, second.id);
}

fn dlq_manager() -> QueueManager {
    let mut manager = manager();
    manager.create_queue(CreateQueue::named("dlq"), 0).unwrap();
    let mut config = manager.queue("q").unwrap().clone();
    config.dead_letter_queue = Some(DeadLetterQueue {
        name: "dlq".into(),
        max_receive_count: 1,
    });
    manager.update_queue(config).unwrap();
    manager
}

#[test]
fn cached_receive_attempt_does_not_recheck_the_dead_letter_threshold() {
    let mut manager = dlq_manager();
    manager
        .send_message("q", SendMessage::body("hello"), 0)
        .unwrap();
    let request = ReceiveMessages {
        count: 1,
        attempt_id: Some("retry".into()),
        ..Default::default()
    };
    let first = manager.receive_messages("q", request.clone(), 0).unwrap();
    let second = manager.receive_messages("q", request, 1).unwrap();
    assert_eq!(second[0].id, first[0].id);
    assert_eq!(second[0].receive_count, 2);
    assert!(manager.messages("dlq").unwrap().is_empty());
    assert!(receive(&mut manager, "q", 1, 30_001).is_empty());
    let dlq = receive(&mut manager, "dlq", 1, 30_001);
    assert_eq!(dlq[0].id, first[0].id);
    assert_eq!(dlq[0].dead_letter_source_queue.as_deref(), Some("q"));
    assert_eq!(dlq[0].created, 0);
}

#[test]
fn deleted_messages_invalidate_receive_attempts() {
    let mut manager = manager();
    manager
        .send_message("q", SendMessage::body("hello"), 0)
        .unwrap();
    let request = ReceiveMessages {
        count: 1,
        attempt_id: Some("retry".into()),
        ..Default::default()
    };
    let first = manager.receive_messages("q", request.clone(), 0).unwrap();
    manager
        .delete_message("q", first[0].receipt_handle.as_ref().unwrap())
        .unwrap();
    assert_eq!(
        manager.receive_messages("q", request, 1),
        Err(Error::AttemptInvalid)
    );
}

#[test]
fn expired_receive_attempts_are_reported_and_removed() {
    let mut manager = manager();
    manager
        .send_message("q", SendMessage::body("hello"), 0)
        .unwrap();
    let request = ReceiveMessages {
        count: 1,
        attempt_id: Some("retry".into()),
        ..Default::default()
    };
    manager.receive_messages("q", request.clone(), 0).unwrap();
    assert_eq!(
        manager.receive_messages("q", request.clone(), 300_001),
        Err(Error::AttemptExpired)
    );
    assert_eq!(
        manager
            .receive_messages("q", request, 300_001)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn an_empty_receive_attempt_does_not_hide_later_messages() {
    let mut manager = manager();
    let request = ReceiveMessages {
        count: 1,
        attempt_id: Some("retry".into()),
        ..Default::default()
    };
    assert!(
        manager
            .receive_messages("q", request.clone(), 0)
            .unwrap()
            .is_empty()
    );
    manager
        .send_message("q", SendMessage::body("hello"), 1)
        .unwrap();
    assert_eq!(manager.receive_messages("q", request, 1).unwrap().len(), 1);
}

#[test]
fn routing_does_not_bind_a_destination_created_later() {
    let mut manager = QueueManager::new();
    manager
        .create_queue(
            CreateQueue {
                copy_messages_to: Some("copy".into()),
                move_messages_to: Some("move".into()),
                ..CreateQueue::named("q")
            },
            0,
        )
        .unwrap();
    for name in ["copy", "move"] {
        manager.create_queue(CreateQueue::named(name), 0).unwrap();
    }
    manager
        .send_message("q", SendMessage::body("hello"), 1)
        .unwrap();
    assert_eq!(manager.messages("q").unwrap().len(), 1);
    for name in ["copy", "move"] {
        assert!(manager.messages(name).unwrap().is_empty());
    }
}

#[test]
fn routes_do_not_rebind_when_a_destination_is_deleted_and_recreated() {
    let mut manager = QueueManager::new();
    manager
        .create_queue(CreateQueue::named("target"), 0)
        .unwrap();
    manager
        .create_queue(
            CreateQueue {
                copy_messages_to: Some("target".into()),
                ..CreateQueue::named("q")
            },
            0,
        )
        .unwrap();
    manager.delete_queue("target");
    manager
        .create_queue(CreateQueue::named("target"), 1)
        .unwrap();
    manager
        .send_message("q", SendMessage::body("hello"), 2)
        .unwrap();
    assert!(manager.messages("target").unwrap().is_empty());
    assert_eq!(manager.messages("q").unwrap().len(), 1);
}

#[test]
fn copies_and_moves_follow_bound_routes() {
    let mut manager = QueueManager::new();
    for name in ["copy", "move"] {
        manager.create_queue(CreateQueue::named(name), 0).unwrap();
    }
    manager
        .create_queue(
            CreateQueue {
                copy_messages_to: Some("copy".into()),
                move_messages_to: Some("move".into()),
                ..CreateQueue::named("q")
            },
            0,
        )
        .unwrap();
    let sent = manager
        .send_message("q", SendMessage::body("hello"), 0)
        .unwrap();
    assert!(manager.messages("q").unwrap().is_empty());
    assert_eq!(manager.messages("move").unwrap()[0].id, sent.id);
    assert_eq!(manager.messages("copy").unwrap()[0].body, "hello");
}

#[test]
fn dead_letter_moves_also_copy_to_the_destination_copy_route() {
    let mut manager = manager();
    manager.create_queue(CreateQueue::named("copy"), 0).unwrap();
    manager
        .create_queue(
            CreateQueue {
                copy_messages_to: Some("copy".into()),
                ..CreateQueue::named("dlq")
            },
            0,
        )
        .unwrap();
    let mut config = manager.queue("q").unwrap().clone();
    config.dead_letter_queue = Some(DeadLetterQueue {
        name: "dlq".into(),
        max_receive_count: 1,
    });
    manager.update_queue(config).unwrap();
    manager
        .send_message("q", SendMessage::body("hello"), 0)
        .unwrap();
    receive(&mut manager, "q", 1, 0);
    receive(&mut manager, "q", 1, 30_001);
    let dlq = manager.messages("dlq").unwrap().remove(0);
    let copy = manager.messages("copy").unwrap().remove(0);
    assert_eq!(copy.id, dlq.id);
    assert_eq!(copy.next_delivery, dlq.next_delivery);
    assert_eq!(copy.dead_letter_source_queue.as_deref(), Some("q"));
    assert_eq!(copy.receive_count, 0);
}

#[test]
fn queue_updates_and_tag_changes_emit_metadata_for_persistence() {
    let mut manager = manager();
    manager.drain_events();
    let mut config = manager.queue("q").unwrap().clone();
    config.visibility_timeout_ms = 10;
    manager.update_queue(config.clone()).unwrap();
    assert_eq!(
        manager.drain_events(),
        vec![QueueEvent::QueueMetadataUpdated(config)]
    );
    manager
        .tag_queue("q", BTreeMap::from([("team".into(), "rust".into())]))
        .unwrap();
    assert!(
        matches!(manager.drain_events().as_slice(), [QueueEvent::QueueMetadataUpdated(q)] if q.tags["team"] == "rust")
    );
    manager.untag_queue("q", &["team".into()]).unwrap();
    assert!(
        matches!(manager.drain_events().as_slice(), [QueueEvent::QueueMetadataUpdated(q)] if q.tags.is_empty())
    );
}

#[test]
fn serialized_engine_preserves_receipts_and_bound_route_identity() {
    let mut manager = dlq_manager();
    manager
        .send_message("q", SendMessage::body("hello"), 0)
        .unwrap();
    let received = receive(&mut manager, "q", 1, 0).remove(0);
    let mut restored: QueueManager =
        serde_json::from_str(&serde_json::to_string(&manager).unwrap()).unwrap();
    assert!(restored.drain_events().is_empty());
    restored
        .delete_message("q", &received.receipt_handle.unwrap())
        .unwrap();
    assert!(restored.messages("q").unwrap().is_empty());
}

fn move_request(source: &str, destination: Option<&str>, rate: Option<u32>) -> StartMoveTask {
    StartMoveTask {
        source: source.into(),
        destination: destination.map(str::to_owned),
        source_arn: format!("arn:aws:sqs:elasticmq:000000000000:{source}"),
        destination_arn: destination
            .map(|name| format!("arn:aws:sqs:elasticmq:000000000000:{name}")),
        max_messages_per_second: rate,
    }
}

#[test]
fn move_tasks_redrive_all_messages_and_record_completion() {
    let mut manager = manager();
    manager
        .create_queue(CreateQueue::named("target"), 0)
        .unwrap();
    for index in 0..3 {
        manager
            .send_message("q", SendMessage::body(index.to_string()), 0)
            .unwrap();
    }
    let handle = manager
        .start_move_task(move_request("q", Some("target"), None), 100)
        .unwrap();
    assert_eq!(manager.list_move_tasks("q").unwrap()[0].to_move, 3);
    for _ in 0..3 {
        assert_eq!(
            manager.step_move_task(&handle, 100).unwrap(),
            Some(std::time::Duration::ZERO)
        );
    }
    assert_eq!(manager.step_move_task(&handle, 100).unwrap(), None);
    assert!(manager.messages("q").unwrap().is_empty());
    assert_eq!(manager.messages("target").unwrap().len(), 3);
    let task = manager.list_move_tasks("q").unwrap().remove(0);
    assert_eq!(task.status, MoveTaskStatus::Completed);
    assert_eq!(task.moved, 3);
    assert_eq!(task.to_move, 0);
    assert!(matches!(
        manager.cancel_move_task(&handle),
        Err(Error::InvalidMoveTaskHandle(_))
    ));
}

#[test]
fn move_tasks_include_delayed_and_invisible_messages_and_reset_receive_stats() {
    let mut manager = manager();
    manager
        .create_queue(CreateQueue::named("target"), 0)
        .unwrap();
    manager
        .send_message("q", SendMessage::body("invisible"), 0)
        .unwrap();
    let first = receive(&mut manager, "q", 1, 0).remove(0);
    manager
        .send_message(
            "q",
            SendMessage {
                delay_ms: Some(90_000),
                ..SendMessage::body("delayed")
            },
            0,
        )
        .unwrap();
    let handle = manager
        .start_move_task(move_request("q", Some("target"), None), 100)
        .unwrap();
    manager.step_move_task(&handle, 100).unwrap();
    manager.step_move_task(&handle, 100).unwrap();
    let messages = manager.messages("target").unwrap();
    let moved = messages.iter().find(|m| m.id == first.id).unwrap();
    assert_eq!(moved.next_delivery, first.next_delivery);
    assert_eq!(moved.created, 100);
    assert_eq!(moved.receive_count, 0);
    assert_eq!(moved.receipt_handle, None);
    assert_eq!(moved.first_receive, None);
    assert!(receive(&mut manager, "target", 10, 100).is_empty());
}

#[test]
fn implicit_move_destination_uses_a_queue_referencing_the_dlq() {
    let mut manager = dlq_manager();
    manager
        .send_message("dlq", SendMessage::body("poison"), 0)
        .unwrap();
    let handle = manager
        .start_move_task(move_request("dlq", None, None), 100)
        .unwrap();
    manager.step_move_task(&handle, 100).unwrap();
    assert_eq!(manager.messages("q").unwrap()[0].body, "poison");
    assert!(manager.messages("dlq").unwrap().is_empty());
}

#[test]
fn move_rate_interval_duplicate_task_and_cancel_are_enforced() {
    let mut manager = manager();
    manager
        .create_queue(CreateQueue::named("target"), 0)
        .unwrap();
    for index in 0..3 {
        manager
            .send_message("q", SendMessage::body(index.to_string()), 0)
            .unwrap();
    }
    let handle = manager
        .start_move_task(move_request("q", Some("target"), Some(1)), 0)
        .unwrap();
    assert!(matches!(
        manager.start_move_task(move_request("q", Some("target"), None), 0),
        Err(Error::MoveTaskAlreadyRunning(_))
    ));
    assert_eq!(
        manager.step_move_task(&handle, 0).unwrap(),
        Some(std::time::Duration::from_secs(1))
    );
    assert_eq!(manager.cancel_move_task(&handle).unwrap(), 1);
    assert_eq!(manager.step_move_task(&handle, 1000).unwrap(), None);
    assert_eq!(manager.messages("q").unwrap().len(), 2);
    assert_eq!(manager.messages("target").unwrap().len(), 1);
    assert!(manager.list_move_tasks("q").unwrap().is_empty());
}

#[test]
fn move_task_history_is_newest_first() {
    let mut manager = manager();
    manager
        .create_queue(CreateQueue::named("target"), 0)
        .unwrap();
    let first = manager
        .start_move_task(move_request("q", Some("target"), None), 0)
        .unwrap();
    manager.step_move_task(&first, 0).unwrap();
    manager
        .send_message("q", SendMessage::body("hello"), 1)
        .unwrap();
    let second = manager
        .start_move_task(move_request("q", Some("target"), Some(1)), 1)
        .unwrap();
    let tasks = manager.list_move_tasks("q").unwrap();
    assert_eq!(tasks[0].handle, second);
    assert_eq!(tasks[0].status, MoveTaskStatus::Running);
    assert_eq!(tasks[1].handle, first);
    assert_eq!(tasks[1].status, MoveTaskStatus::Completed);
}

#[test]
fn deleting_and_recreating_move_destination_does_not_rebind_the_task() {
    let mut manager = manager();
    manager
        .create_queue(CreateQueue::named("target"), 0)
        .unwrap();
    manager
        .send_message("q", SendMessage::body("hello"), 0)
        .unwrap();
    let handle = manager
        .start_move_task(move_request("q", Some("target"), None), 0)
        .unwrap();
    manager.delete_queue("target");
    manager
        .create_queue(CreateQueue::named("target"), 1)
        .unwrap();
    assert_eq!(
        manager.step_move_task(&handle, 1).unwrap(),
        Some(std::time::Duration::ZERO)
    );
    assert_eq!(manager.step_move_task(&handle, 1).unwrap(), None);
    assert!(manager.messages("q").unwrap().is_empty());
    assert!(manager.messages("target").unwrap().is_empty());
    assert_eq!(
        manager.list_move_tasks("q").unwrap()[0].status,
        MoveTaskStatus::Completed
    );
}

#[test]
fn fifo_move_tasks_preserve_order_and_group_metadata() {
    let mut manager = QueueManager::new();
    fifo(&mut manager, "source.fifo");
    fifo(&mut manager, "target.fifo");
    for body in ["one", "two"] {
        manager
            .send_message("source.fifo", grouped(body, "g"), 0)
            .unwrap();
    }
    let handle = manager
        .start_move_task(move_request("source.fifo", Some("target.fifo"), None), 100)
        .unwrap();
    while manager.step_move_task(&handle, 100).unwrap().is_some() {}
    let messages = receive(&mut manager, "target.fifo", 10, 100);
    assert_eq!(
        messages.iter().map(|m| m.body.as_str()).collect::<Vec<_>>(),
        ["one", "two"]
    );
    assert!(messages.iter().all(|m| m.group_id.as_deref() == Some("g")));
}
