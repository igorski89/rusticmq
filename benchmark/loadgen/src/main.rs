//! Raw SQS JSON load generator. One keep-alive connection per worker, no request
//! signing and minimal parsing, so the server rather than the client is measured.
//! Each command prints one JSON object; `benchmark/compare.py` drives it.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::Request;
use hyper::client::conn::http1::{SendRequest, handshake};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

struct Client {
    host: String,
    sender: Option<SendRequest<Full<Bytes>>>,
}

impl Client {
    fn new(host: &str) -> Self {
        Self {
            host: host.into(),
            sender: None,
        }
    }

    async fn sender(&mut self) -> Result<&mut SendRequest<Full<Bytes>>, String> {
        if self.sender.as_ref().is_none_or(|s| s.is_closed()) {
            let stream = TcpStream::connect(&self.host)
                .await
                .map_err(|e| e.to_string())?;
            stream.set_nodelay(true).map_err(|e| e.to_string())?;
            let (sender, connection) = handshake(TokioIo::new(stream))
                .await
                .map_err(|e| e.to_string())?;
            tokio::spawn(connection);
            self.sender = Some(sender);
        }
        Ok(self.sender.as_mut().unwrap())
    }

    async fn request(&mut self, request: Request<Full<Bytes>>) -> Result<(u16, Bytes), String> {
        let sender = self.sender().await?;
        let sent = async {
            sender.ready().await?;
            let response = sender.send_request(request).await?;
            let status = response.status().as_u16();
            let body = response.into_body().collect().await?.to_bytes();
            Ok::<_, hyper::Error>((status, body))
        };
        match sent.await {
            Ok(result) => Ok(result),
            Err(error) => {
                self.sender = None;
                Err(error.to_string())
            }
        }
    }

    async fn post(&mut self, action: &str, body: String) -> Result<Bytes, String> {
        let request = Request::post("/")
            .header("host", self.host.as_str())
            .header("content-type", "application/x-amz-json-1.0")
            .header("x-amz-target", format!("AmazonSQS.{action}"))
            .body(Full::new(Bytes::from(body)))
            .map_err(|e| e.to_string())?;
        let (status, body) = self.request(request).await?;
        if status >= 400 {
            let text = String::from_utf8_lossy(&body[..body.len().min(300)]).into_owned();
            return Err(format!("{action} {status}: {text}"));
        }
        Ok(body)
    }
}

struct Args(HashMap<String, String>);

impl Args {
    fn parse() -> (String, Self) {
        let mut raw = std::env::args().skip(1);
        let command = raw.next().unwrap_or_default();
        let mut values = HashMap::new();
        while let Some(key) = raw.next() {
            let key = key.trim_start_matches("--").to_string();
            values.insert(key, raw.next().unwrap_or_default());
        }
        (command, Self(values))
    }

    fn string(&self, key: &str) -> String {
        self.0
            .get(key)
            .unwrap_or_else(|| panic!("--{key} is required"))
            .clone()
    }

    fn number(&self, key: &str, default: u64) -> u64 {
        self.0.get(key).map_or(default, |v| v.parse().unwrap())
    }

    fn flag(&self, key: &str) -> bool {
        self.0.get(key).is_some_and(|v| v == "true")
    }

    fn queues(&self) -> Vec<String> {
        self.string("queues").split(',').map(String::from).collect()
    }
}

fn epoch_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

/// VM-wide busy and total jiffies. A container's /proc/stat reports the whole VM.
fn vm_cpu() -> (u64, u64) {
    let stat = std::fs::read_to_string("/proc/stat").unwrap_or_default();
    let fields: Vec<u64> = stat
        .lines()
        .next()
        .unwrap_or_default()
        .split_whitespace()
        .skip(1)
        .filter_map(|f| f.parse().ok())
        .collect();
    let idle = fields.get(3).copied().unwrap_or(0) + fields.get(4).copied().unwrap_or(0);
    let total: u64 = fields.iter().sum();
    (total - idle, total)
}

fn percentiles(mut samples: Vec<u64>) -> Value {
    if samples.is_empty() {
        return Value::Null;
    }
    samples.sort_unstable();
    let at = |p: f64| {
        let index = ((samples.len() as f64 * p).ceil() as usize).clamp(1, samples.len()) - 1;
        samples[index] as f64 / 1000.0
    };
    json!({"p50_ms": at(0.50), "p90_ms": at(0.90), "p99_ms": at(0.99), "p999_ms": at(0.999),
        "max_ms": at(1.0), "samples": samples.len()})
}

fn send_body(
    queue: &str,
    first: u64,
    count: u64,
    stamp: u64,
    padding: &str,
    fifo: bool,
) -> (&'static str, String) {
    let entry = |seq: u64| {
        let group = if fifo {
            format!(
                r#","MessageGroupId":"g{}","MessageDeduplicationId":"d{seq}""#,
                seq % 8
            )
        } else {
            String::new()
        };
        (format!(r#""MessageBody":"{seq}:{stamp}:{padding}""#), group)
    };
    if count == 1 {
        let (body, group) = entry(first);
        return (
            "SendMessage",
            format!(r#"{{"QueueUrl":"{queue}",{body}{group}}}"#),
        );
    }
    let entries: Vec<String> = (first..first + count)
        .map(|seq| {
            let (body, group) = entry(seq);
            format!(r#"{{"Id":"{seq}",{body}{group}}}"#)
        })
        .collect();
    (
        "SendMessageBatch",
        format!(
            r#"{{"QueueUrl":"{queue}","Entries":[{}]}}"#,
            entries.join(",")
        ),
    )
}

/// `seq:micros:padding` padded to the requested body size.
fn padding(body_bytes: u64) -> Arc<String> {
    Arc::new("x".repeat(body_bytes.saturating_sub(32) as usize))
}

#[derive(Default)]
struct Totals {
    latencies: Vec<u64>,
    acked: Vec<u64>,
    errors: u64,
    first_error: Option<String>,
}

impl Totals {
    fn error(&mut self, error: String) {
        self.errors += 1;
        self.first_error.get_or_insert(error);
    }

    fn merge(&mut self, other: Totals) {
        self.latencies.extend(other.latencies);
        self.acked.extend(other.acked);
        self.errors += other.errors;
        if self.first_error.is_none() {
            self.first_error = other.first_error;
        }
    }
}

/// Closed loop: each connection sends as fast as the server answers.
async fn send(args: &Args, host: &str) -> Value {
    let queues = args.queues();
    let connections = args.number("conns", 8);
    let batch = args.number("batch", 1);
    let limit = args.number("count", u64::MAX);
    let duration = Duration::from_millis(args.number("duration-ms", 3_600_000));
    let fifo = args.flag("fifo");
    let record = args.flag("record");
    let padding = padding(args.number("body-bytes", 256));
    let next = Arc::new(AtomicU64::new(args.number("start-seq", 0)));
    let end = args.number("start-seq", 0).saturating_add(limit);
    let started = Instant::now();
    let mut workers = Vec::new();
    for worker in 0..connections {
        let queue = queues[worker as usize % queues.len()].clone();
        let (next, padding, host) = (next.clone(), padding.clone(), host.to_string());
        workers.push(tokio::spawn(async move {
            let mut client = Client::new(&host);
            let mut totals = Totals::default();
            while started.elapsed() < duration {
                let first = next.fetch_add(batch, Ordering::Relaxed);
                if first >= end {
                    break;
                }
                let count = batch.min(end - first);
                let (action, body) =
                    send_body(&queue, first, count, epoch_micros(), &padding, fifo);
                let call = Instant::now();
                match client.post(action, body).await {
                    Ok(reply) => {
                        totals.latencies.push(call.elapsed().as_micros() as u64);
                        if action == "SendMessageBatch" {
                            let reply: Value = serde_json::from_slice(&reply).unwrap_or_default();
                            if reply["Failed"].as_array().is_some_and(|f| !f.is_empty()) {
                                totals.error(format!("batch failures: {reply}"));
                                continue;
                            }
                        }
                        totals.acked.extend(first..first + count);
                    }
                    Err(error) => {
                        totals.error(error);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
            }
            totals
        }));
    }
    let mut totals = Totals::default();
    for worker in workers {
        totals.merge(worker.await.unwrap());
    }
    let elapsed = started.elapsed().as_secs_f64();
    let sent = totals.acked.len();
    let mut output = json!({"sent": sent, "errors": totals.errors, "first_error": totals.first_error,
        "seconds": elapsed, "messages_per_second": sent as f64 / elapsed,
        "latency": percentiles(totals.latencies), "highest_seq": next.load(Ordering::Relaxed)});
    if record {
        output["acked"] = json!(totals.acked);
    }
    output
}

/// Receive and delete until the expected messages arrived, the time cap passed or,
/// with `idle-ms`, the queues stayed empty that long. Asserts exactly-once delivery.
async fn consume(args: &Args, host: &str) -> Value {
    let queues = args.queues();
    let connections = args.number("conns", 8);
    let expect = args.number("expect", u64::MAX);
    let cap = Duration::from_millis(args.number("max-ms", 60_000));
    let idle = args.number("idle-ms", 0);
    let record = args.flag("record");
    let seen = Arc::new(Mutex::new(HashSet::<u64>::new()));
    let duplicates = Arc::new(AtomicU64::new(0));
    let received = Arc::new(AtomicU64::new(0));
    let last_message = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let mut workers = Vec::new();
    for worker in 0..connections {
        let queue = queues[worker as usize % queues.len()].clone();
        let (seen, duplicates, received, last_message, host) = (
            seen.clone(),
            duplicates.clone(),
            received.clone(),
            last_message.clone(),
            host.to_string(),
        );
        workers.push(tokio::spawn(async move {
            let mut client = Client::new(&host);
            let mut receives = Vec::new();
            let mut deletes = Vec::new();
            let mut totals = Totals::default();
            while received.load(Ordering::Relaxed) < expect && started.elapsed() < cap {
                let body = format!(r#"{{"QueueUrl":"{queue}","MaxNumberOfMessages":10,"WaitTimeSeconds":0}}"#);
                let call = Instant::now();
                let reply = match client.post("ReceiveMessage", body).await {
                    Ok(reply) => reply,
                    Err(error) => {
                        totals.error(error);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        continue;
                    }
                };
                receives.push(call.elapsed().as_micros() as u64);
                let reply: Value = serde_json::from_slice(&reply).unwrap_or_default();
                let messages = reply["Messages"].as_array().cloned().unwrap_or_default();
                if messages.is_empty() {
                    let quiet = started.elapsed().as_millis() as u64 - last_message.load(Ordering::Relaxed);
                    if idle > 0 && quiet >= idle {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    continue;
                }
                last_message.store(started.elapsed().as_millis() as u64, Ordering::Relaxed);
                {
                    let mut seen = seen.lock().await;
                    for message in &messages {
                        let seq = message["Body"]
                            .as_str()
                            .and_then(|b| b.split(':').next())
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(u64::MAX);
                        if !seen.insert(seq) {
                            duplicates.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                let entries: Vec<Value> = messages
                    .iter()
                    .enumerate()
                    .map(|(id, m)| json!({"Id": id.to_string(), "ReceiptHandle": m["ReceiptHandle"]}))
                    .collect();
                let body = json!({"QueueUrl": queue, "Entries": entries}).to_string();
                let call = Instant::now();
                match client.post("DeleteMessageBatch", body).await {
                    Ok(_) => {
                        deletes.push(call.elapsed().as_micros() as u64);
                        received.fetch_add(messages.len() as u64, Ordering::Relaxed);
                    }
                    Err(error) => totals.error(error),
                }
            }
            (receives, deletes, totals)
        }));
    }
    let (mut receives, mut deletes, mut totals) = (Vec::new(), Vec::new(), Totals::default());
    for worker in workers {
        let (r, d, t) = worker.await.unwrap();
        receives.extend(r);
        deletes.extend(d);
        totals.merge(t);
    }
    let count = received.load(Ordering::Relaxed);
    // The trailing idle wait is not work the server did.
    let elapsed = if idle > 0 {
        last_message.load(Ordering::Relaxed) as f64 / 1000.0
    } else {
        started.elapsed().as_secs_f64()
    };
    let unique = seen.lock().await;
    let mut output = json!({"received": count, "unique": unique.len(), "duplicates": duplicates.load(Ordering::Relaxed),
        "errors": totals.errors, "first_error": totals.first_error, "seconds": elapsed,
        "complete": expect == u64::MAX || count >= expect,
        "messages_per_second": count as f64 / elapsed.max(0.001),
        "receive_latency": percentiles(receives), "delete_latency": percentiles(deletes)});
    if record {
        output["seen"] = json!(unique.iter().collect::<Vec<_>>());
    }
    output
}

/// Open loop at a fixed rate. Send latency counts from each message's scheduled
/// time, so a stalled server is charged for the sends it delayed. End-to-end latency
/// runs from the same schedule to the consumer's receipt, with long-polling consumers.
async fn rate(args: &Args, host: &str) -> Value {
    let queue = args.queues().remove(0);
    let per_second = args.number("rate", 1000);
    let duration_ms = args.number("duration-ms", 15_000);
    let total = per_second * duration_ms / 1000;
    let padding = padding(args.number("body-bytes", 256));
    let next = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let start_us = epoch_micros() + 100_000;
    let started = Instant::now() + Duration::from_micros(100_000);
    let mut consumers = Vec::new();
    for _ in 0..args.number("consumers", 4) {
        let (queue, host, done) = (queue.clone(), host.to_string(), done.clone());
        consumers.push(tokio::spawn(async move {
            let mut client = Client::new(&host);
            let mut samples = Vec::new();
            let mut totals = Totals::default();
            loop {
                let body = format!(r#"{{"QueueUrl":"{queue}","MaxNumberOfMessages":10,"WaitTimeSeconds":1}}"#);
                let reply = match client.post("ReceiveMessage", body).await {
                    Ok(reply) => reply,
                    Err(error) => {
                        totals.error(error);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        continue;
                    }
                };
                let received = epoch_micros();
                let reply: Value = serde_json::from_slice(&reply).unwrap_or_default();
                let messages = reply["Messages"].as_array().cloned().unwrap_or_default();
                if messages.is_empty() {
                    if done.load(Ordering::Relaxed) {
                        break;
                    }
                    continue;
                }
                for message in &messages {
                    let stamp = message["Body"]
                        .as_str()
                        .and_then(|b| b.split(':').nth(1))
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(received);
                    samples.push(received.saturating_sub(stamp));
                }
                let entries: Vec<Value> = messages
                    .iter()
                    .enumerate()
                    .map(|(id, m)| json!({"Id": id.to_string(), "ReceiptHandle": m["ReceiptHandle"]}))
                    .collect();
                let body = json!({"QueueUrl": queue, "Entries": entries}).to_string();
                if let Err(error) = client.post("DeleteMessageBatch", body).await {
                    totals.error(error);
                }
            }
            (samples, totals)
        }));
    }
    let mut senders = Vec::new();
    for _ in 0..args.number("conns", 64) {
        let (queue, host, next, padding) = (
            queue.clone(),
            host.to_string(),
            next.clone(),
            padding.clone(),
        );
        senders.push(tokio::spawn(async move {
            let mut client = Client::new(&host);
            let mut totals = Totals::default();
            loop {
                let seq = next.fetch_add(1, Ordering::Relaxed);
                if seq >= total {
                    break;
                }
                let offset = seq * 1_000_000 / per_second;
                let due = started + Duration::from_micros(offset);
                tokio::time::sleep_until(due.into()).await;
                let (action, body) = send_body(&queue, seq, 1, start_us + offset, &padding, false);
                match client.post(action, body).await {
                    Ok(_) => {
                        totals
                            .latencies
                            .push(Instant::now().saturating_duration_since(due).as_micros() as u64);
                        totals.acked.push(seq);
                    }
                    Err(error) => totals.error(error),
                }
            }
            totals
        }));
    }
    let mut sent = Totals::default();
    for sender in senders {
        sent.merge(sender.await.unwrap());
    }
    let send_seconds = (Instant::now().saturating_duration_since(started)).as_secs_f64();
    done.store(true, Ordering::Relaxed);
    let mut end_to_end = Vec::new();
    let mut errors = sent.errors;
    let mut first_error = sent.first_error;
    for consumer in consumers {
        let (samples, totals) = consumer.await.unwrap();
        end_to_end.extend(samples);
        errors += totals.errors;
        if first_error.is_none() {
            first_error = totals.first_error;
        }
    }
    json!({"offered_per_second": per_second, "sent": sent.acked.len(), "received": end_to_end.len(),
        "achieved_per_second": sent.acked.len() as f64 / send_seconds, "errors": errors, "first_error": first_error,
        "send_latency": percentiles(sent.latencies), "end_to_end_latency": percentiles(end_to_end)})
}

/// Hold long polls open on an empty queue, for the runner to sample idle cost.
async fn polls(args: &Args, host: &str) -> Value {
    let queue = args.queues().remove(0);
    let hold = Duration::from_millis(args.number("duration-ms", 10_000));
    let returned = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let mut workers = Vec::new();
    for _ in 0..args.number("conns", 1000) {
        let (queue, host, returned, failed) = (
            queue.clone(),
            host.to_string(),
            returned.clone(),
            failed.clone(),
        );
        workers.push(tokio::spawn(async move {
            let mut client = Client::new(&host);
            let body =
                format!(r#"{{"QueueUrl":"{queue}","MaxNumberOfMessages":1,"WaitTimeSeconds":20}}"#);
            match client.post("ReceiveMessage", body).await {
                Ok(_) => returned.fetch_add(1, Ordering::Relaxed),
                Err(_) => failed.fetch_add(1, Ordering::Relaxed),
            };
        }));
    }
    tokio::time::sleep(hold).await;
    json!({"returned_early": returned.load(Ordering::Relaxed), "failed": failed.load(Ordering::Relaxed)})
}

async fn wait_health(host: &str, timeout_ms: u64) -> Value {
    let started = Instant::now();
    while started.elapsed() < Duration::from_millis(timeout_ms) {
        let mut client = Client::new(host);
        let request = Request::get("/health")
            .header("host", host)
            .body(Full::new(Bytes::new()))
            .unwrap();
        if let Ok((200, _)) = client.request(request).await {
            return json!({"healthy": true, "epoch_ms": epoch_micros() / 1000});
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    json!({"healthy": false})
}

async fn call(host: &str, action: &str, body: String) -> Value {
    match Client::new(host).post(action, body).await {
        Ok(reply) => serde_json::from_slice(&reply).unwrap_or(json!({})),
        Err(error) => json!({"error": error}),
    }
}

#[tokio::main]
async fn main() {
    let (command, args) = Args::parse();
    if command == "sleep" {
        std::future::pending::<()>().await;
    }
    let host = args.string("host");
    let cpu_before = vm_cpu();
    let mut output = match command.as_str() {
        "health" => wait_health(&host, args.number("timeout-ms", 120_000)).await,
        "idle" => {
            tokio::time::sleep(Duration::from_millis(args.number("duration-ms", 2000))).await;
            json!({})
        }
        "create" => {
            let mut attributes = json!({"VisibilityTimeout": "600"});
            if args.flag("fifo") {
                attributes["FifoQueue"] = json!("true");
            }
            let body = json!({"QueueName": args.string("name"), "Attributes": attributes});
            call(&host, "CreateQueue", body.to_string()).await
        }
        "delete" => {
            let body = json!({"QueueUrl": args.string("queue")});
            call(&host, "DeleteQueue", body.to_string()).await
        }
        "list" => call(&host, "ListQueues", "{}".into()).await,
        "depth" => {
            let body = json!({"QueueUrl": args.string("queue"),
                "AttributeNames": ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]});
            call(&host, "GetQueueAttributes", body.to_string()).await
        }
        "send" => send(&args, &host).await,
        "consume" => consume(&args, &host).await,
        "rate" => rate(&args, &host).await,
        "polls" => polls(&args, &host).await,
        other => panic!("Unknown command: {other}"),
    };
    let cpu_after = vm_cpu();
    let total = (cpu_after.1 - cpu_before.1).max(1) as f64;
    output["vm_cpu_busy_percent"] = json!(100.0 * (cpu_after.0 - cpu_before.0) as f64 / total);
    println!("{output}");
    std::process::exit(0);
}
