use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use rusticmq::{
    QueueManager,
    sqs::{ServerConfig, SqsService},
};
use serde_json::{Value, json};
use tower::ServiceExt;

fn app() -> Router {
    SqsService::new(QueueManager::new(), ServerConfig::default()).router()
}

#[tokio::test]
async fn auto_created_queues_use_template_routes_tags_and_fifo_defaults() {
    use rusticmq::{CreateQueue, ReceiveMessages};
    let mut manager = QueueManager::new();
    manager
        .create_queue(CreateQueue::named("audit"), 0)
        .unwrap();
    manager
        .create_queue(CreateQueue::named("template"), 0)
        .unwrap();
    let mut template = manager.queue("template").unwrap().clone();
    manager.delete_queue("template");
    template.visibility_timeout_ms = 2000;
    template.copy_messages_to = Some("audit".into());
    template.tags.insert("team".into(), "rust".into());
    template.content_based_deduplication = true;
    let service = SqsService::new(
        manager,
        ServerConfig {
            auto_create_template: Some(template),
            ..Default::default()
        },
    );
    let manager = service.manager.clone();
    let app = service.router();
    let url = "http://localhost:9324/000000000000/automatic.fifo";
    ok(
        &app,
        "SendMessage",
        json!({"QueueUrl":url,"MessageBody":"created", "MessageGroupId":"g"}),
    )
    .await;
    let attributes = ok(
        &app,
        "GetQueueAttributes",
        json!({"QueueUrl":url,"AttributeNames":["All"]}),
    )
    .await;
    assert_eq!(attributes["Attributes"]["VisibilityTimeout"], "2");
    assert_eq!(attributes["Attributes"]["FifoQueue"], "true");
    assert_eq!(
        ok(&app, "ListQueueTags", json!({"QueueUrl":url})).await["Tags"]["team"],
        "rust"
    );
    let mut manager = manager.lock().await;
    assert_eq!(
        manager
            .receive_messages("audit", ReceiveMessages::count(1), rusticmq::sqs::now())
            .unwrap()[0]
            .body,
        "created"
    );
}

#[tokio::test]
async fn receive_and_lookup_can_auto_create_queues() {
    use rusticmq::CreateQueue;
    let mut manager = QueueManager::new();
    let template = manager
        .create_queue(CreateQueue::named("template"), 0)
        .unwrap();
    manager.delete_queue("template");
    let app = SqsService::new(
        manager,
        ServerConfig {
            auto_create_template: Some(template),
            ..Default::default()
        },
    )
    .router();
    assert_eq!(
        ok(&app, "GetQueueUrl", json!({"QueueName":"lookup"})).await["QueueUrl"],
        "http://localhost:9324/000000000000/lookup"
    );
    assert_eq!(
        ok(
            &app,
            "ReceiveMessage",
            json!({"QueueUrl":"/000000000000/received"})
        )
        .await["Messages"],
        json!([])
    );
    let urls = ok(&app, "ListQueues", json!({})).await;
    assert_eq!(urls["QueueUrls"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn request_bodies_follow_the_configured_max_content_length() {
    let body = json!({"QueueUrl":"q","MessageBody":"a".repeat(9 * 1024 * 1024)});
    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/")
                .header("x-amz-target", "AmazonSQS.SendMessage")
                .header("content-type", "application/x-amz-json-1.0")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let app = SqsService::new(
        QueueManager::new(),
        ServerConfig {
            relaxed_limits: true,
            max_content_length: Some(500 * 1024 * 1024),
            ..Default::default()
        },
    )
    .router();
    let url = create(&app, "large").await;
    let body = "a".repeat(21 * 1024 * 1024);
    ok(
        &app,
        "SendMessage",
        json!({"QueueUrl":url,"MessageBody":body}),
    )
    .await;
    let received = ok(&app, "ReceiveMessage", json!({"QueueUrl":url})).await;
    assert_eq!(
        received["Messages"][0]["Body"].as_str().unwrap().len(),
        body.len()
    );
}

#[tokio::test]
async fn relaxed_limits_accept_large_batches_counts_and_empty_attributes() {
    let app = SqsService::new(
        QueueManager::new(),
        ServerConfig {
            relaxed_limits: true,
            ..Default::default()
        },
    )
    .router();
    let url = create(&app, &"q".repeat(100)).await;
    let attributes = json!({"empty":{"DataType":"String","StringValue":""},"number":{"DataType":"Number","StringValue":"1e200"}});
    let entries: Vec<_> = (0..12)
        .map(|id| json!({"Id":id.to_string(),"MessageBody":"", "MessageAttributes":attributes}))
        .collect();
    let sent = ok(
        &app,
        "SendMessageBatch",
        json!({"QueueUrl":url,"Entries":entries}),
    )
    .await;
    assert_eq!(sent["Successful"].as_array().unwrap().len(), 12);
    let received = ok(
        &app,
        "ReceiveMessage",
        json!({"QueueUrl":url,"MaxNumberOfMessages":12}),
    )
    .await;
    assert_eq!(received["Messages"].as_array().unwrap().len(), 12);
    assert_eq!(
        ok(
            &app,
            "ReceiveMessage",
            json!({"QueueUrl":url,"MaxNumberOfMessages":0})
        )
        .await["Messages"],
        json!([])
    );
    assert_eq!(
        call(&app, "CreateQueue", json!({"QueueName":"bad/name"}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn an_oversized_batch_fails_before_any_message_is_sent() {
    let app = app();
    let url = create(&app, "oversized").await;
    let (status, value) = call(
        &app,
        "SendMessageBatch",
        json!({"QueueUrl":url,"Entries":[
            {"Id":"a", "MessageBody":"x".repeat(600_000)},
            {"Id":"b", "MessageBody":"x".repeat(600_000)}
        ]}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["__type"], "com.amazonaws.sqs#InvalidAttributeValue");
    assert_eq!(
        ok(&app, "ReceiveMessage", json!({"QueueUrl":url})).await["Messages"],
        json!([])
    );
}

async fn scrape(app: &Router) -> String {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(body.to_vec()).unwrap()
}

#[tokio::test]
async fn metrics_report_traffic_deduplication_and_configuration() {
    let app = SqsService::new(
        QueueManager::new(),
        ServerConfig {
            config_file: Some("/opt/elasticmq.conf".into()),
            configured_queues: 4,
            persistent_storage: true,
            max_content_length: None,
            ..Default::default()
        },
    )
    .router();
    let body = scrape(&app).await;
    assert!(body.contains(&format!(
        "rusticmq_build_info{{version=\"{}\",commit=\"",
        env!("CARGO_PKG_VERSION")
    )));
    assert!(body.contains(
        "rusticmq_config_info{file=\"/opt/elasticmq.conf\",storage=\"sqlite\",sqs_limits=\"strict\"} 1"
    ));
    assert!(body.contains("rusticmq_config_queues 4\n"));
    assert!(body.contains("rusticmq_config_max_content_length_bytes +Inf\n"));

    let url = ok(
        &app,
        "CreateQueue",
        json!({"QueueName":"traffic.fifo","Attributes":{"FifoQueue":"true"}}),
    )
    .await["QueueUrl"]
        .as_str()
        .unwrap()
        .to_owned();
    let body = scrape(&app).await;
    assert!(body.contains("rusticmq_messages_sent_total{queue=\"traffic.fifo\"} 0"));
    assert!(!body.contains("rusticmq_queue_last_send_timestamp_seconds{"));
    assert!(!body.contains("rusticmq_queue_last_receive_timestamp_seconds{"));

    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    for (body, id) in [("first", "a"), ("again", "a"), ("second", "b")] {
        ok(
            &app,
            "SendMessage",
            json!({"QueueUrl":url,"MessageBody":body,"MessageGroupId":"g","MessageDeduplicationId":id}),
        )
        .await;
    }
    let received = ok(
        &app,
        "ReceiveMessage",
        json!({"QueueUrl":url,"MaxNumberOfMessages":10}),
    )
    .await;
    let messages = received["Messages"].as_array().unwrap();
    assert_eq!(messages.len(), 2);
    ok(
        &app,
        "DeleteMessage",
        json!({"QueueUrl":url,"ReceiptHandle":messages[0]["ReceiptHandle"]}),
    )
    .await;
    let empty = ok(&app, "ReceiveMessage", json!({"QueueUrl":url})).await;
    assert!(empty["Messages"].as_array().is_none_or(Vec::is_empty));

    let body = scrape(&app).await;
    for expected in [
        "rusticmq_messages_sent_total{queue=\"traffic.fifo\"} 2",
        "rusticmq_messages_deduplicated_total{queue=\"traffic.fifo\"} 1",
        "rusticmq_messages_received_total{queue=\"traffic.fifo\"} 2",
        "rusticmq_messages_deleted_total{queue=\"traffic.fifo\"} 1",
        "# TYPE rusticmq_messages_sent_total counter",
    ] {
        assert!(body.contains(expected), "{expected} missing from\n{body}");
    }
    for metric in [
        "rusticmq_queue_last_send_timestamp_seconds",
        "rusticmq_queue_last_receive_timestamp_seconds",
    ] {
        let prefix = format!("{metric}{{queue=\"traffic.fifo\"}} ");
        let value: f64 = body
            .lines()
            .find_map(|line| line.strip_prefix(&prefix))
            .unwrap()
            .parse()
            .unwrap();
        assert!(
            value >= before.floor() && value < before + 60.0,
            "{metric} {value}"
        );
    }
}

#[tokio::test]
async fn metrics_report_delayed_messages_and_remove_deleted_queues() {
    let app = app();
    let url = create(&app, "metrics").await;
    ok(
        &app,
        "SendMessage",
        json!({"QueueUrl":url,"MessageBody":"later", "DelaySeconds":900}),
    )
    .await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["content-type"],
        "text/plain; version=0.0.4; charset=utf-8"
    );
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("rusticmq_messages_delayed{queue=\"metrics\"} 1"));
    assert!(body.contains("rusticmq_messages_visible{queue=\"metrics\"} 0"));
    ok(&app, "DeleteQueue", json!({"QueueUrl":url})).await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert!(
        !std::str::from_utf8(&body)
            .unwrap()
            .contains("queue=\"metrics\"")
    );
}

#[tokio::test]
async fn wildcard_addresses_follow_the_request_host_and_context() {
    let app = SqsService::new(
        QueueManager::new(),
        ServerConfig {
            context_path: "/sqs".into(),
            wildcard_queue_urls: true,
            ..Default::default()
        },
    )
    .router();
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sqs/000000000000")
                .header("host", "queue.example:9999")
                .header("x-amz-target", "AmazonSQS.CreateQueue")
                .header("content-type", "application/x-amz-json-1.0")
                .body(Body::from(json!({"QueueName":"wildcard"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let response: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        response["QueueUrl"],
        "http://queue.example:9999/sqs/000000000000/wildcard"
    );
}

#[tokio::test]
async fn queue_urls_require_the_configured_context_and_accept_account_aliases() {
    use rusticmq::CreateQueue;
    let mut manager = QueueManager::new();
    manager.create_queue(CreateQueue::named("jobs"), 0).unwrap();
    let app = SqsService::new(
        manager,
        ServerConfig {
            context_path: "/xyz".into(),
            ..Default::default()
        },
    )
    .router();
    for (url, status) in [
        ("http://localhost/xyz/123456789012/jobs", StatusCode::OK),
        ("http://localhost/xyz/queue/jobs", StatusCode::OK),
        ("http://localhost/xyz/abc/jobs", StatusCode::OK),
        ("http://localhost/xyz/jobs", StatusCode::OK),
        ("http://localhost/jobs", StatusCode::BAD_REQUEST),
        ("http://localhost/xyz/a/b/jobs", StatusCode::BAD_REQUEST),
        ("http://localhost/xyz/a-b/jobs", StatusCode::BAD_REQUEST),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/xyz")
                    .header("x-amz-target", "AmazonSQS.SendMessage")
                    .header("content-type", "application/x-amz-json-1.0")
                    .body(Body::from(
                        json!({"QueueUrl":url,"MessageBody":"hello"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), status, "{url}");
    }
}

#[tokio::test]
async fn legacy_redrive_policies_accept_plain_queue_names_and_string_counts() {
    let app = app();
    let target = create(&app, "legacy-dlq").await;
    let source = ok(&app, "CreateQueue", json!({"QueueName":"legacy-source","Attributes":{
        "VisibilityTimeout":"0", "RedrivePolicy":json!({"deadLetterTargetArn":"legacy-dlq", "maxReceiveCount":"1"}).to_string()
    }})).await["QueueUrl"].as_str().unwrap().to_owned();
    ok(
        &app,
        "SendMessage",
        json!({"QueueUrl":source,"MessageBody":"redrive"}),
    )
    .await;
    assert_eq!(
        ok(&app, "ReceiveMessage", json!({"QueueUrl":source})).await["Messages"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        ok(&app, "ReceiveMessage", json!({"QueueUrl":source})).await["Messages"],
        json!([])
    );
    assert_eq!(
        ok(&app, "ReceiveMessage", json!({"QueueUrl":target})).await["Messages"][0]["Body"],
        "redrive"
    );
}

#[tokio::test]
async fn binary_attributes_accept_upstream_signed_byte_arrays() {
    let app = app();
    let url = create(&app, "binary-array").await;
    let attributes = json!({"binary":{"DataType":"Binary", "BinaryValue":[-128,0,127,-1]}});
    let sent = ok(
        &app,
        "SendMessage",
        json!({"QueueUrl":url,"MessageBody":"bytes", "MessageAttributes":attributes}),
    )
    .await;
    let received = ok(
        &app,
        "ReceiveMessage",
        json!({"QueueUrl":url,"MessageAttributeNames":["All"]}),
    )
    .await;
    assert_eq!(
        received["Messages"][0]["MessageAttributes"]["binary"]["BinaryValue"],
        "gAB//w=="
    );
    assert_eq!(
        received["Messages"][0]["MD5OfMessageAttributes"],
        sent["MD5OfMessageAttributes"]
    );
    assert_eq!(call(&app, "SendMessage", json!({"QueueUrl":url,"MessageBody":"bad", "MessageAttributes":{"bad":{"DataType":"Binary","BinaryValue":[128]}}})).await.0, StatusCode::BAD_REQUEST);
}

async fn call(app: &Router, action: &str, payload: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/")
                .header("x-amz-target", format!("AmazonSQS.{action}"))
                .header("content-type", "application/x-amz-json-1.0")
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

async fn ok(app: &Router, action: &str, payload: Value) -> Value {
    let (status, value) = call(app, action, payload).await;
    assert_eq!(status, StatusCode::OK, "{value}");
    value
}

async fn create(app: &Router, name: &str) -> String {
    ok(app, "CreateQueue", json!({"QueueName":name})).await["QueueUrl"]
        .as_str()
        .unwrap()
        .into()
}

#[tokio::test]
async fn json_lifecycle_with_attributes_and_binary_md5() {
    let app = app();
    let url = create(&app, "q").await;
    let attributes = json!({"greeting":{"DataType":"String.custom","StringValue":"héllo"},
        "count":{"DataType":"Number","StringValue":"1.234567890123456789"},
        "binary":{"DataType":"Binary","BinaryValue":"AAH/"}});
    let sent = ok(
        &app,
        "SendMessage",
        json!({"QueueUrl":url,"MessageBody":"hello","MessageAttributes":attributes}),
    )
    .await;
    assert_eq!(sent["MD5OfMessageBody"], "5d41402abc4b2a76b9719d911017c592");
    let received = ok(
        &app,
        "ReceiveMessage",
        json!({"QueueUrl":url,"AttributeNames":["All"],"MessageAttributeNames":["All"]}),
    )
    .await;
    let message = &received["Messages"][0];
    assert_eq!(message["MessageId"], sent["MessageId"]);
    assert_eq!(message["MessageAttributes"], attributes);
    assert_eq!(
        message["MD5OfMessageAttributes"],
        sent["MD5OfMessageAttributes"]
    );
    assert_eq!(message["Attributes"]["ApproximateReceiveCount"], "1");
    ok(
        &app,
        "DeleteMessage",
        json!({"QueueUrl":url,"ReceiptHandle":message["ReceiptHandle"]}),
    )
    .await;
    assert_eq!(
        ok(&app, "ReceiveMessage", json!({"QueueUrl":url})).await["Messages"],
        json!([])
    );
}

#[tokio::test]
async fn batches_return_partial_failures_without_losing_valid_entries() {
    let app = app();
    let url = create(&app, "q").await;
    let batch = ok(
        &app,
        "SendMessageBatch",
        json!({"QueueUrl":url,"Entries":[
        {"Id":"good","MessageBody":"hello"},{"Id":"bad","MessageBody":""}]}),
    )
    .await;
    assert_eq!(batch["Successful"][0]["Id"], "good");
    assert_eq!(batch["Failed"][0]["Id"], "bad");
    assert_eq!(batch["Failed"][0]["SenderFault"], true);
    let message = ok(&app, "ReceiveMessage", json!({"QueueUrl":url})).await["Messages"][0].clone();
    let deleted = ok(&app, "DeleteMessageBatch", json!({"QueueUrl":url,"Entries":[
        {"Id":"good","ReceiptHandle":message["ReceiptHandle"]},{"Id":"bad","ReceiptHandle":"not-valid"}]})).await;
    assert_eq!(deleted["Successful"][0]["Id"], "good");
    assert_eq!(deleted["Failed"][0]["Code"], "ReceiptHandleIsInvalid");
}

#[tokio::test]
async fn duplicate_batch_ids_fail_before_any_send() {
    let app = app();
    let url = create(&app, "q").await;
    let (status, error) = call(
        &app,
        "SendMessageBatch",
        json!({"QueueUrl":url,"Entries":[
        {"Id":"same","MessageBody":"a"},{"Id":"same","MessageBody":"b"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error["__type"],
        "com.amazonaws.sqs#BatchEntryIdsNotDistinct"
    );
    assert_eq!(
        ok(&app, "ReceiveMessage", json!({"QueueUrl":url})).await["Messages"],
        json!([])
    );
}

#[tokio::test]
async fn fifo_deduplication_and_group_lock_work_over_http() {
    let app = app();
    let url = ok(&app, "CreateQueue", json!({"QueueName":"q.fifo","Attributes":{"FifoQueue":"true","ContentBasedDeduplication":"true"}})).await["QueueUrl"].clone();
    let send = json!({"QueueUrl":url,"MessageBody":"first","MessageGroupId":"g"});
    let first = ok(&app, "SendMessage", send.clone()).await;
    assert_eq!(
        ok(&app, "SendMessage", send).await["MessageId"],
        first["MessageId"]
    );
    ok(
        &app,
        "SendMessage",
        json!({"QueueUrl":url,"MessageBody":"second","MessageGroupId":"g"}),
    )
    .await;
    let received = ok(
        &app,
        "ReceiveMessage",
        json!({"QueueUrl":url,"MaxNumberOfMessages":1}),
    )
    .await;
    assert_eq!(received["Messages"][0]["Body"], "first");
    assert_eq!(
        ok(&app, "ReceiveMessage", json!({"QueueUrl":url})).await["Messages"],
        json!([])
    );
    ok(
        &app,
        "DeleteMessage",
        json!({"QueueUrl":url,"ReceiptHandle":received["Messages"][0]["ReceiptHandle"]}),
    )
    .await;
    assert_eq!(
        ok(&app, "ReceiveMessage", json!({"QueueUrl":url})).await["Messages"][0]["Body"],
        "second"
    );
}

#[tokio::test]
async fn receive_attempt_retries_bypass_dead_letter_routing() {
    let app = app();
    let dlq = create(&app, "dlq").await;
    let url = create(&app, "q").await;
    let policy =
        json!({"deadLetterTargetArn":"arn:aws:sqs:elasticmq:000000000000:dlq","maxReceiveCount":1})
            .to_string();
    ok(
        &app,
        "SetQueueAttributes",
        json!({"QueueUrl":url,"Attributes":{"RedrivePolicy":policy}}),
    )
    .await;
    ok(
        &app,
        "SendMessage",
        json!({"QueueUrl":url,"MessageBody":"hello"}),
    )
    .await;
    let request = json!({"QueueUrl":url,"ReceiveRequestAttemptId":"retry","VisibilityTimeout":0});
    let first = ok(&app, "ReceiveMessage", request.clone()).await;
    let second = ok(&app, "ReceiveMessage", request).await;
    assert_eq!(
        first["Messages"][0]["MessageId"],
        second["Messages"][0]["MessageId"]
    );
    assert_eq!(
        ok(&app, "ReceiveMessage", json!({"QueueUrl":url})).await["Messages"],
        json!([])
    );
    assert_eq!(
        ok(
            &app,
            "ReceiveMessage",
            json!({"QueueUrl":dlq,"AttributeNames":["All"]})
        )
        .await["Messages"][0]["Attributes"]["DeadLetterQueueSourceArn"],
        "arn:aws:sqs:elasticmq:000000000000:q"
    );
}

#[tokio::test]
async fn queue_attributes_tags_and_prefix_listing() {
    let app = app();
    let url = create(&app, "prefix-q").await;
    create(&app, "other").await;
    ok(
        &app,
        "SetQueueAttributes",
        json!({"QueueUrl":url,"Attributes":{"VisibilityTimeout":"5","DelaySeconds":"2"}}),
    )
    .await;
    let attrs = ok(
        &app,
        "GetQueueAttributes",
        json!({"QueueUrl":url,"AttributeNames":["VisibilityTimeout"]}),
    )
    .await;
    assert_eq!(attrs, json!({"Attributes":{"VisibilityTimeout":"5"}}));
    ok(
        &app,
        "TagQueue",
        json!({"QueueUrl":url,"Tags":{"a":"one","b":"two"}}),
    )
    .await;
    ok(&app, "UntagQueue", json!({"QueueUrl":url,"TagKeys":["a"]})).await;
    assert_eq!(
        ok(&app, "ListQueueTags", json!({"QueueUrl":url})).await["Tags"],
        json!({"b":"two"})
    );
    assert_eq!(
        ok(&app, "ListQueues", json!({"QueueNamePrefix":"prefix-"})).await["QueueUrls"],
        json!([url])
    );
}

#[tokio::test]
async fn long_poll_wakes_when_another_request_sends() {
    let app = app();
    let url = create(&app, "q").await;
    let receive_app = app.clone();
    let receive_url = url.clone();
    let waiting = tokio::spawn(async move {
        ok(
            &receive_app,
            "ReceiveMessage",
            json!({"QueueUrl":receive_url,"WaitTimeSeconds":2}),
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    ok(
        &app,
        "SendMessage",
        json!({"QueueUrl":url,"MessageBody":"wake"}),
    )
    .await;
    let response = tokio::time::timeout(std::time::Duration::from_millis(500), waiting)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response["Messages"][0]["Body"], "wake");
}

#[tokio::test]
async fn simultaneous_long_polls_do_not_receive_the_same_message() {
    let app = app();
    let url = create(&app, "q").await;
    let first_app = app.clone();
    let first_url = url.clone();
    let first = tokio::spawn(async move {
        ok(
            &first_app,
            "ReceiveMessage",
            json!({"QueueUrl":first_url,"WaitTimeSeconds":1}),
        )
        .await
    });
    let second_app = app.clone();
    let second_url = url.clone();
    let second = tokio::spawn(async move {
        ok(
            &second_app,
            "ReceiveMessage",
            json!({"QueueUrl":second_url,"WaitTimeSeconds":1}),
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    ok(
        &app,
        "SendMessage",
        json!({"QueueUrl":url,"MessageBody":"only one"}),
    )
    .await;
    let count = first.await.unwrap()["Messages"].as_array().unwrap().len()
        + second.await.unwrap()["Messages"].as_array().unwrap().len();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn invalid_json_shapes_return_errors_without_panicking() {
    let app = app();
    for payload in [json!(null), json!([]), json!("text"), json!(123)] {
        let (status, _) = call(&app, "ListQueues", payload).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}

#[tokio::test]
async fn numbers_are_validated_with_decimal_precision() {
    let app = app();
    let url = create(&app, "q").await;
    // f64 rounds the out-of-range integer to exactly 10^126.
    let above = format!("1{}1", "0".repeat(125));
    let (status, _) = call(
        &app,
        "SendMessage",
        json!({"QueueUrl":url,"MessageBody":"hello",
        "MessageAttributes":{"number":{"DataType":"Number","StringValue":above}}}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    ok(
        &app,
        "SendMessage",
        json!({"QueueUrl":url,"MessageBody":"hello",
        "MessageAttributes":{"number":{"DataType":"Number","StringValue":"1e126"}}}),
    )
    .await;
}

async fn query(app: &Router, params: Vec<(&str, String)>) -> (StatusCode, String) {
    let request = Request::builder()
        .method("POST")
        .uri("/")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Body::from(serde_urlencoded::to_string(params).unwrap()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

#[tokio::test]
async fn query_xml_escapes_content_and_preserves_cr_characters() {
    let app = app();
    let (status, created) = query(
        &app,
        vec![("Action", "CreateQueue".into()), ("QueueName", "q".into())],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(created.contains("<QueueUrl>http://localhost:9324/000000000000/q</QueueUrl>"));
    let url = "http://localhost:9324/000000000000/q".to_owned();
    query(
        &app,
        vec![
            ("Action", "SendMessage".into()),
            ("QueueUrl", url.clone()),
            ("MessageBody", "<&\r\n雪".into()),
        ],
    )
    .await;
    let (status, received) = query(
        &app,
        vec![("Action", "ReceiveMessage".into()), ("QueueUrl", url)],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(received.contains("<Body>&lt;&amp;&#13;\n雪</Body>"));
}

#[tokio::test]
async fn query_batch_and_attribute_maps_are_decoded() {
    let app = app();
    let url = create(&app, "q").await;
    let (status, result) = query(
        &app,
        vec![
            ("Action", "SendMessageBatch".into()),
            ("QueueUrl", url.clone()),
            ("SendMessageBatchRequestEntry.1.Id", "one".into()),
            ("SendMessageBatchRequestEntry.1.MessageBody", "hello".into()),
            (
                "SendMessageBatchRequestEntry.1.MessageAttribute.1.Name",
                "a".into(),
            ),
            (
                "SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.DataType",
                "String".into(),
            ),
            (
                "SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.StringValue",
                "value".into(),
            ),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(result.contains("<SendMessageBatchResultEntry>"));
    let message = ok(
        &app,
        "ReceiveMessage",
        json!({"QueueUrl":url,"MessageAttributeNames":["All"]}),
    )
    .await;
    assert_eq!(
        message["Messages"][0]["MessageAttributes"]["a"]["StringValue"],
        "value"
    );
}

#[tokio::test]
async fn health_and_cors_preflight_work() {
    let app = app();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "OK"
    );
    let response = app
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(response.headers()["access-control-allow-origin"], "*");
}

#[tokio::test]
async fn message_move_task_finishes_asynchronously_and_lists_completion() {
    let app = app();
    let source = create(&app, "source").await;
    let target = create(&app, "target").await;
    for index in 0..3 {
        ok(
            &app,
            "SendMessage",
            json!({"QueueUrl":source,"MessageBody":index.to_string()}),
        )
        .await;
    }
    let source_arn = "arn:aws:sqs:elasticmq:000000000000:source";
    let started = ok(&app, "StartMessageMoveTask", json!({"SourceArn":source_arn,"DestinationArn":"arn:aws:sqs:elasticmq:000000000000:target"})).await;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let task = ok(
                &app,
                "ListMessageMoveTasks",
                json!({"SourceArn":source_arn}),
            )
            .await;
            if task["Results"][0]["Status"] == "COMPLETED" {
                assert_eq!(task["Results"][0]["TaskHandle"], started["TaskHandle"]);
                assert_eq!(task["Results"][0]["ApproximateNumberOfMessagesMoved"], 3);
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        ok(
            &app,
            "ReceiveMessage",
            json!({"QueueUrl":target,"MaxNumberOfMessages":10})
        )
        .await["Messages"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        ok(&app, "ReceiveMessage", json!({"QueueUrl":source})).await["Messages"],
        json!([])
    );
}

#[tokio::test]
async fn a_rate_limited_move_task_can_be_cancelled_before_all_messages_move() {
    let app = app();
    let source = create(&app, "source").await;
    create(&app, "target").await;
    for index in 0..3 {
        ok(
            &app,
            "SendMessage",
            json!({"QueueUrl":source,"MessageBody":index.to_string()}),
        )
        .await;
    }
    let request = json!({"SourceArn":"arn:aws:sqs:elasticmq:000000000000:source", "DestinationArn":"arn:aws:sqs:elasticmq:000000000000:target", "MaxNumberOfMessagesPerSecond":1});
    let handle = ok(&app, "StartMessageMoveTask", request.clone()).await["TaskHandle"].clone();
    let (status, error) = call(&app, "StartMessageMoveTask", request).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error["__type"], "com.amazonaws.sqs#UnsupportedOperation");
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    let cancelled = ok(&app, "CancelMessageMoveTask", json!({"TaskHandle":handle})).await;
    assert_eq!(cancelled["ApproximateNumberOfMessagesMoved"], 1);
    tokio::time::sleep(std::time::Duration::from_millis(1050)).await;
    assert_eq!(
        ok(
            &app,
            "ReceiveMessage",
            json!({"QueueUrl":source,"MaxNumberOfMessages":10})
        )
        .await["Messages"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn invalid_task_handles_and_arns_return_sdk_error_shapes() {
    let app = app();
    let (status, error) = call(
        &app,
        "CancelMessageMoveTask",
        json!({"TaskHandle":"missing"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error["__type"],
        "com.amazonaws.sqs#ResourceNotFoundException"
    );
    let (status, error) = call(&app, "ListMessageMoveTasks", json!({"SourceArn":"bad-arn"})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error["__type"], "com.amazonaws.sqs#QueueDoesNotExist");
}

#[tokio::test]
async fn shutdown_finishes_existing_and_new_relaxed_long_polls() {
    let service = SqsService::new(
        QueueManager::new(),
        ServerConfig {
            relaxed_limits: true,
            ..Default::default()
        },
    );
    let app = service.clone().router();
    let url = create(&app, "q").await;
    let waiting_app = app.clone();
    let request = json!({"QueueUrl":url,"WaitTimeSeconds":86400});
    let waiting_request = request.clone();
    let waiting =
        tokio::spawn(async move { ok(&waiting_app, "ReceiveMessage", waiting_request).await });
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert!(!waiting.is_finished());
    service.shutdown();
    let result = tokio::time::timeout(std::time::Duration::from_millis(500), waiting)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result["Messages"], json!([]));
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        ok(&app, "ReceiveMessage", request),
    )
    .await
    .unwrap();
    assert_eq!(result["Messages"], json!([]));
}

#[tokio::test]
async fn move_worker_recovers_when_initial_persistence_fails() {
    use rusticmq::{CreateQueue, SendMessage, persistence::Persistence};
    let root = tempfile::tempdir().unwrap();
    let parent = root.path().join("unavailable");
    let store = Persistence::open(
        Some(parent.join("queues.conf")),
        Some(":memory:".into()),
        false,
    )
    .unwrap();
    let mut manager = QueueManager::new();
    manager
        .create_queue(CreateQueue::named("source"), 0)
        .unwrap();
    manager
        .create_queue(CreateQueue::named("target"), 0)
        .unwrap();
    manager
        .send_message("source", SendMessage::body("recover"), 0)
        .unwrap();
    let service = SqsService::new(manager, ServerConfig::default()).with_persistence(store);
    let app = service.clone().router();
    let request = json!({"SourceArn":"arn:aws:sqs:elasticmq:000000000000:source", "DestinationArn":"arn:aws:sqs:elasticmq:000000000000:target"});
    let (status, error) = call(&app, "StartMessageMoveTask", request.clone()).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(error["__type"], "com.amazonaws.sqs#InternalError");
    {
        let manager = service.manager.lock().await;
        assert_eq!(manager.messages("source").unwrap().len(), 1);
        assert!(manager.messages("target").unwrap().is_empty());
        assert_eq!(
            manager.list_move_tasks("source").unwrap()[0].status,
            rusticmq::MoveTaskStatus::Running
        );
    }
    std::fs::create_dir(&parent).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let manager = service.manager.lock().await;
            if manager.list_move_tasks("source").unwrap()[0].status
                == rusticmq::MoveTaskStatus::Completed
            {
                assert!(manager.messages("source").unwrap().is_empty());
                assert_eq!(manager.messages("target").unwrap()[0].body, "recover");
                break;
            }
            drop(manager);
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    ok(&app, "StartMessageMoveTask", request).await;
}

#[tokio::test]
async fn long_poll_wakes_at_delayed_delivery_without_another_request() {
    use rusticmq::{CreateQueue, SendMessage};
    let mut manager = QueueManager::new();
    manager.create_queue(CreateQueue::named("q"), 0).unwrap();
    manager
        .send_message(
            "q",
            SendMessage {
                delay_ms: Some(100),
                ..SendMessage::body("delayed")
            },
            rusticmq::sqs::now(),
        )
        .unwrap();
    let app = SqsService::new(manager, ServerConfig::default()).router();
    let response = tokio::time::timeout(
        std::time::Duration::from_millis(750),
        ok(
            &app,
            "ReceiveMessage",
            json!({"QueueUrl":"/000000000000/q", "WaitTimeSeconds":2}),
        ),
    )
    .await
    .unwrap();
    assert_eq!(response["Messages"][0]["Body"], "delayed");
}
