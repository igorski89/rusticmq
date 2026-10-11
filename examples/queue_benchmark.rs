//! Run with `cargo run --release --example queue_benchmark -- 10000 50000`.
use rusticmq::*;
use std::time::Instant;

fn main() {
    let sizes: Vec<usize> = std::env::args()
        .skip(1)
        .map(|s| s.parse().unwrap())
        .collect();
    for size in if sizes.is_empty() {
        vec![10_000, 50_000]
    } else {
        sizes
    } {
        for fifo in [false, true] {
            let mut manager = QueueManager::new();
            let name = if fifo { "bench.fifo" } else { "bench" };
            manager
                .create_queue(
                    CreateQueue {
                        fifo,
                        content_based_deduplication: fifo,
                        ..CreateQueue::named(name)
                    },
                    0,
                )
                .unwrap();
            // Preload through the same restore path as SQLite to isolate receive
            // cost from send-time hashing and deduplication maintenance.
            let sample = manager
                .send_message(
                    name,
                    SendMessage {
                        group_id: fifo.then(|| "0".into()),
                        ..SendMessage::body("benchmark")
                    },
                    0,
                )
                .unwrap();
            manager.purge_queue(name).unwrap();
            let messages = (0..size)
                .map(|i| {
                    let mut data = serde_json::to_value(&sample).unwrap();
                    data["id"] = serde_json::json!(i.to_string());
                    data["order"] = serde_json::json!(i);
                    data["group_id"] = serde_json::json!(fifo.then(|| (i % 8).to_string()));
                    data["deduplication_id"] = serde_json::Value::Null;
                    serde_json::from_value(data).unwrap()
                })
                .collect();
            manager.restore_messages(name, messages).unwrap();
            manager.drain_events();
            let start = Instant::now();
            let mut delivered = 0;
            for _ in 0..100 {
                let batch = manager
                    .receive_messages(name, ReceiveMessages::count(10), 1)
                    .unwrap();
                delivered += batch.len();
                manager.drain_events();
            }
            println!(
                "{} depth={size}: 100 receives, {delivered} delivered, {:.3} ms",
                if fifo { "FIFO" } else { "standard" },
                start.elapsed().as_secs_f64() * 1000.0
            );
            // Make every message invisible, then time empty long-poll probes.
            let mut messages = manager.messages(name).unwrap();
            for message in &mut messages {
                message.next_delivery = 60_000;
            }
            manager.restore_messages(name, messages).unwrap();
            manager.drain_events();
            let start = Instant::now();
            for _ in 0..100 {
                assert!(
                    manager
                        .receive_messages(name, ReceiveMessages::count(10), 1)
                        .unwrap()
                        .is_empty()
                );
            }
            println!(
                "  100 empty probes: {:.3} ms",
                start.elapsed().as_secs_f64() * 1000.0
            );
        }
    }
}
