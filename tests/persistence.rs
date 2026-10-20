use rusticmq::{
    config::{AppConfig, read_queue_file},
    persistence::Persistence,
    *,
};
use std::{collections::BTreeMap, fs};

fn flush(store: &Persistence, manager: &mut QueueManager) {
    let events = manager.drain_events();
    store.apply(&events, manager).unwrap();
}

#[test]
fn sqlite_memory_is_the_default_and_configuration_accepts_file_paths() {
    let config = AppConfig::load(None, 0).unwrap();
    assert_eq!(config.message_storage.unwrap().to_str(), Some(":memory:"));
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("elasticmq.conf");
    fs::write(
        &path,
        r#"
      rest-sqs { bind-hostname = "127.0.0.1", bind-port = 0 }
      node-address { host = "localhost", port = 12345, context-path = "sqs" }
      messages-storage { enabled = true, uri = "sqlite:state.db" }
      queues { test { defaultVisibilityTimeout = 2 seconds } }
    "#,
    )
    .unwrap();
    let config = AppConfig::load(Some(&path), 100).unwrap();
    assert_eq!(config.message_storage.unwrap().to_str(), Some("state.db"));
    assert_eq!(config.bind, "127.0.0.1:0");
    assert_eq!(config.service.base_url, "http://localhost:12345/sqs");
    assert_eq!(config.queues[0].visibility_timeout_ms, 2000);
}

#[test]
fn hocon_reads_auto_creation_templates_relaxed_limits_and_root_context() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("elasticmq.conf");
    fs::write(
        &path,
        r#"
        rest-sqs { sqs-limits = relaxed }
        node-address { context-path = "/" }
        auto-create-queues {
            enabled = true
            template {
                defaultVisibilityTimeout = 2 seconds
                contentBasedDeduplication = true
                tags { team = "rust" }
            }
        }
    "#,
    )
    .unwrap();
    let config = AppConfig::load(Some(&path), 0).unwrap();
    assert!(config.service.relaxed_limits);
    assert_eq!(config.service.context_path, "");
    let template = config.service.auto_create_template.unwrap();
    assert_eq!(template.visibility_timeout_ms, 2000);
    assert!(template.content_based_deduplication);
    assert_eq!(template.tags["team"], "rust");
}

#[test]
fn hocon_reads_pekko_max_content_length() {
    assert_eq!(
        AppConfig::load(None, 0).unwrap().service.max_content_length,
        Some(8 * 1024 * 1024)
    );
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("elasticmq.conf");
    for (conf, expected) in [
        (
            "pekko { http { server { parsing { max-content-length = 500m } } } }",
            Some(500 * 1024 * 1024),
        ),
        (
            "pekko.http.server.parsing.max-content-length = 1024",
            Some(1024),
        ),
        (
            "pekko.http.server.parsing.max-content-length = infinite",
            None,
        ),
        (
            "pekko.http.client.parsing.max-content-length = 1m",
            Some(8 * 1024 * 1024),
        ),
    ] {
        fs::write(&path, conf).unwrap();
        let config = AppConfig::load(Some(&path), 0).unwrap();
        assert_eq!(config.service.max_content_length, expected, "{conf}");
    }
    fs::write(&path, "pekko.http.server.parsing.max-content-length = lots").unwrap();
    assert!(AppConfig::load(Some(&path), 0).is_err());
}

#[test]
fn sqlite_restart_restores_receipts_statistics_attributes_and_visibility() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state.sqlite");
    let received;
    {
        let store = Persistence::open(None, Some(path.clone()), false).unwrap();
        let mut manager = store.restore(Vec::new(), 0).unwrap();
        manager.create_queue(CreateQueue::named("q"), 0).unwrap();
        manager
            .send_message(
                "q",
                SendMessage {
                    attributes: BTreeMap::from([(
                        "binary".into(),
                        MessageAttribute {
                            value: AttributeValue::Binary(vec![0, 255]),
                            custom_type: Some("raw".into()),
                        },
                    )]),
                    tracing_id: Some("trace".into()),
                    ..SendMessage::body("hello")
                },
                0,
            )
            .unwrap();
        received = manager
            .receive_messages("q", ReceiveMessages::count(1), 100)
            .unwrap()
            .remove(0);
        manager
            .tag_queue("q", BTreeMap::from([("team".into(), "rust".into())]))
            .unwrap();
        flush(&store, &mut manager);
    }
    let store = Persistence::open(None, Some(path.clone()), false).unwrap();
    let mut restored = store.restore(Vec::new(), 200).unwrap();
    assert_eq!(restored.messages("q").unwrap(), vec![received.clone()]);
    assert_eq!(restored.queue("q").unwrap().tags["team"], "rust");
    assert!(
        restored
            .receive_messages("q", ReceiveMessages::count(1), 200)
            .unwrap()
            .is_empty()
    );
    restored
        .delete_message("q", received.receipt_handle.as_ref().unwrap())
        .unwrap();
    flush(&store, &mut restored);
    drop(store);
    let store = Persistence::open(None, Some(path), false).unwrap();
    assert!(
        store
            .restore(Vec::new(), 300)
            .unwrap()
            .messages("q")
            .unwrap()
            .is_empty()
    );
}

#[test]
fn purge_and_queue_deletion_are_durable() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state.sqlite");
    {
        let store = Persistence::open(None, Some(path.clone()), false).unwrap();
        let mut manager = store.restore(Vec::new(), 0).unwrap();
        for name in ["purged", "deleted"] {
            manager.create_queue(CreateQueue::named(name), 0).unwrap();
            manager
                .send_message(name, SendMessage::body("hello"), 0)
                .unwrap();
        }
        flush(&store, &mut manager);
        manager.purge_queue("purged").unwrap();
        manager.delete_queue("deleted");
        flush(&store, &mut manager);
    }
    let restored = Persistence::open(None, Some(path), false)
        .unwrap()
        .restore(Vec::new(), 1)
        .unwrap();
    assert_eq!(restored.list_queues(None), ["purged"]);
    assert!(restored.messages("purged").unwrap().is_empty());
}

#[test]
fn sqlite_fifo_restart_preserves_order_group_blocking_deduplication_and_sequence() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("fifo.sqlite");
    let send = |body: &str, group: &str| SendMessage {
        group_id: Some(group.into()),
        ..SendMessage::body(body)
    };
    let (first, tail, independent, receipt);
    {
        let store = Persistence::open(None, Some(path.clone()), false).unwrap();
        let mut manager = store.restore(Vec::new(), 0).unwrap();
        manager
            .create_queue(
                CreateQueue {
                    fifo: true,
                    content_based_deduplication: true,
                    ..CreateQueue::named("q.fifo")
                },
                0,
            )
            .unwrap();
        first = manager
            .send_message("q.fifo", send("head", "a"), 0)
            .unwrap();
        tail = manager
            .send_message("q.fifo", send("tail", "a"), 0)
            .unwrap();
        independent = manager
            .send_message("q.fifo", send("independent", "b"), 0)
            .unwrap();
        receipt = manager
            .receive_messages("q.fifo", ReceiveMessages::count(1), 0)
            .unwrap()
            .remove(0)
            .receipt_handle
            .unwrap();
        flush(&store, &mut manager);
    }
    let store = Persistence::open(None, Some(path), false).unwrap();
    let mut manager = store.restore(Vec::new(), 1).unwrap();
    assert_eq!(
        manager
            .send_message("q.fifo", send("head", "a"), 1)
            .unwrap()
            .id,
        first.id
    );
    let other = manager
        .receive_messages("q.fifo", ReceiveMessages::count(10), 1)
        .unwrap();
    assert_eq!(other.len(), 1);
    assert_eq!(other[0].id, independent.id);
    manager.delete_message("q.fifo", &receipt).unwrap();
    let next = manager.send_message("q.fifo", send("new", "a"), 1).unwrap();
    assert!(
        next.sequence_number
            .as_ref()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            > independent
                .sequence_number
                .as_ref()
                .unwrap()
                .parse::<u64>()
                .unwrap()
    );
    let group = manager
        .receive_messages("q.fifo", ReceiveMessages::count(10), 1)
        .unwrap();
    assert_eq!(
        group.iter().map(|message| &message.id).collect::<Vec<_>>(),
        [&tail.id, &next.id]
    );
}

#[test]
fn file_mode_stores_definitions_and_routes_without_messages() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("queues.conf");
    let store = Persistence::open(Some(path.clone()), None, false).unwrap();
    let mut manager = QueueManager::new();
    manager
        .create_queue(CreateQueue::named("z-target"), 0)
        .unwrap();
    manager
        .create_queue(
            CreateQueue {
                copy_messages_to: Some("z-target".into()),
                fifo: true,
                content_based_deduplication: true,
                tags: BTreeMap::from([("tag".into(), "雪".into())]),
                ..CreateQueue::named("a-source.fifo")
            },
            0,
        )
        .unwrap();
    manager
        .send_message(
            "a-source.fifo",
            SendMessage {
                group_id: Some("g".into()),
                ..SendMessage::body("hello")
            },
            0,
        )
        .unwrap();
    flush(&store, &mut manager);
    let definitions = read_queue_file(&path, 1).unwrap();
    assert_eq!(definitions.len(), 2);
    let mut restored = store.restore(Vec::new(), 1).unwrap();
    assert!(restored.messages("z-target").unwrap().is_empty());
    assert!(restored.messages("a-source.fifo").unwrap().is_empty());
    assert_eq!(restored.queue("a-source.fifo").unwrap().tags["tag"], "雪");
    restored
        .send_message(
            "a-source.fifo",
            SendMessage {
                group_id: Some("g".into()),
                ..SendMessage::body("new")
            },
            1,
        )
        .unwrap();
    assert_eq!(restored.messages("z-target").unwrap().len(), 1);
}

#[test]
fn persisted_definitions_override_base_queue_defaults() {
    let store = Persistence::open(None, Some(":memory:".into()), false).unwrap();
    let mut manager = QueueManager::new();
    let persisted = manager
        .create_queue(
            CreateQueue {
                visibility_timeout_ms: Some(500),
                ..CreateQueue::named("q")
            },
            0,
        )
        .unwrap();
    flush(&store, &mut manager);
    let mut base = QueueManager::new();
    let default = base.create_queue(CreateQueue::named("q"), 100).unwrap();
    let extra = base.create_queue(CreateQueue::named("extra"), 100).unwrap();
    let restored = store.restore(vec![default, extra], 100).unwrap();
    assert_eq!(restored.queue("q").unwrap(), &persisted);
    assert!(restored.queue("extra").is_ok());
}

#[test]
fn malformed_storage_is_reported_instead_of_discarded() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("queues.conf");
    fs::write(
        &path,
        "queues { q { defaultVisibilityTimeout = nonsense } }",
    )
    .unwrap();
    let store = Persistence::open(Some(path), None, false).unwrap();
    assert!(store.restore(Vec::new(), 0).is_err());
}

#[test]
fn hocon_fifo_suffix_durations_and_dependency_cycles() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("queues.conf");
    fs::write(&path,r#"queues {
        a { fifo = true, defaultVisibilityTimeout = 250 milliseconds, delay = 5 seconds, copyTo = "z" }
        z {}
    }"#).unwrap();
    let definitions = read_queue_file(&path, 100).unwrap();
    let mut manager = QueueManager::new();
    manager.restore_queues(definitions).unwrap();
    assert_eq!(manager.queue("a.fifo").unwrap().visibility_timeout_ms, 250);
    assert_eq!(manager.queue("a.fifo").unwrap().delay_ms, 5000);
    let mut cyclic = manager.queue("z").unwrap().clone();
    cyclic.copy_messages_to = Some("z".into());
    let mut empty = QueueManager::new();
    assert!(empty.restore_queues(vec![cyclic]).is_err());
    assert!(empty.list_queues(None).is_empty());
}

#[test]
fn sqlite_prune_removes_queues_and_their_messages() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state.sqlite");
    {
        let store = Persistence::open(None, Some(path.clone()), false).unwrap();
        let mut manager = QueueManager::new();
        manager.create_queue(CreateQueue::named("q"), 0).unwrap();
        manager
            .send_message("q", SendMessage::body("hello"), 0)
            .unwrap();
        flush(&store, &mut manager);
    }
    let store = Persistence::open(None, Some(path), true).unwrap();
    assert!(
        store
            .restore(Vec::new(), 100)
            .unwrap()
            .list_queues(None)
            .is_empty()
    );
}
