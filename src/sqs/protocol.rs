use super::{ApiError, ApiResult};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub fn query_request(params: BTreeMap<String, String>) -> ApiResult<(String, Value)> {
    let action = params
        .get("Action")
        .cloned()
        .ok_or_else(|| ApiError::new("MissingAction", "Action is missing"))?;
    let mut request = query_object(&params);
    if action.ends_with("Batch") {
        let prefix = format!("{action}RequestEntry.");
        let indices: BTreeSet<_> = params
            .keys()
            .filter_map(|key| key.strip_prefix(&prefix))
            .filter_map(|tail| tail.split('.').next()?.parse::<usize>().ok())
            .collect();
        let entries: Vec<_> = indices
            .into_iter()
            .map(|index| {
                let prefix = format!("{prefix}{index}.");
                let entry = params
                    .iter()
                    .filter_map(|(key, value)| {
                        key.strip_prefix(&prefix)
                            .map(|k| (k.to_owned(), value.clone()))
                    })
                    .collect();
                Value::Object(query_object(&entry))
            })
            .collect();
        request.insert("Entries".into(), json!(entries));
    }
    Ok((action, Value::Object(request)))
}

fn query_object(params: &BTreeMap<String, String>) -> Map<String, Value> {
    let mut object = Map::new();
    for (key, value) in params {
        if !key.contains('.') && key != "Action" && key != "Version" {
            let number = matches!(
                key.as_str(),
                "DelaySeconds"
                    | "MaxNumberOfMessages"
                    | "VisibilityTimeout"
                    | "WaitTimeSeconds"
                    | "MaxResults"
                    | "MaxNumberOfMessagesPerSecond"
            );
            let parsed = if number {
                value
                    .parse::<i64>()
                    .map(Value::from)
                    .unwrap_or_else(|_| Value::String(value.clone()))
            } else {
                Value::String(value.clone())
            };
            object.insert(key.clone(), parsed);
        }
    }
    for (prefix, field) in [("Attribute", "Attributes"), ("Tag", "Tags")] {
        let mut values = Map::new();
        for (key, value) in params {
            if let Some(index) = key
                .strip_prefix(&format!("{prefix}."))
                .and_then(|s| s.strip_suffix(if prefix == "Tag" { ".Key" } else { ".Name" }))
                && let Some(v) = params.get(&format!("{prefix}.{index}.Value"))
            {
                values.insert(value.clone(), Value::String(v.clone()));
            }
        }
        if !values.is_empty() {
            object.insert(field.into(), Value::Object(values));
        }
    }
    for (prefix, field) in [
        ("AttributeName", "AttributeNames"),
        ("MessageSystemAttributeName", "MessageSystemAttributeNames"),
        ("MessageAttributeName", "MessageAttributeNames"),
        ("TagKey", "TagKeys"),
    ] {
        let mut values: Vec<_> = params
            .iter()
            .filter_map(|(key, value)| {
                key.strip_prefix(&format!("{prefix}."))
                    .and_then(|index| index.parse::<usize>().ok())
                    .map(|index| (index, value.clone()))
            })
            .collect();
        values.sort_by_key(|(index, _)| *index);
        if !values.is_empty() {
            object.insert(
                field.into(),
                json!(values.into_iter().map(|(_, v)| v).collect::<Vec<_>>()),
            );
        }
    }
    for (prefix, field) in [
        ("MessageAttribute", "MessageAttributes"),
        ("MessageSystemAttribute", "MessageSystemAttributes"),
    ] {
        let mut values = Map::new();
        for (key, name) in params {
            if let Some(index) = key
                .strip_prefix(&format!("{prefix}."))
                .and_then(|s| s.strip_suffix(".Name"))
            {
                let value_prefix = format!("{prefix}.{index}.Value.");
                let fields = params
                    .iter()
                    .filter_map(|(key, value)| {
                        key.strip_prefix(&value_prefix)
                            .map(|k| (k.into(), Value::String(value.clone())))
                    })
                    .collect();
                values.insert(name.clone(), Value::Object(fields));
            }
        }
        if !values.is_empty() {
            object.insert(field.into(), Value::Object(values));
        }
    }
    object
}

pub fn xml_response(action: &str, result: &Value, request_id: &str) -> String {
    let content = result
        .as_object()
        .map(|map| {
            map.iter()
                .map(|(key, value)| xml_field(key, value, action))
                .collect::<String>()
        })
        .unwrap_or_default();
    format!(
        "<{action}Response xmlns=\"http://queue.amazonaws.com/doc/2012-11-05/\"><{action}Result>{content}</{action}Result><ResponseMetadata><RequestId>{request_id}</RequestId></ResponseMetadata></{action}Response>"
    )
}

pub fn xml_error(error: &ApiError, request_id: &str) -> String {
    format!(
        "<ErrorResponse><Error><Type>Sender</Type><Code>{}</Code><Message>{}</Message><Detail/></Error><RequestId>{request_id}</RequestId></ErrorResponse>",
        escape(&error.code),
        escape(&error.message)
    )
}

fn xml_field(key: &str, value: &Value, action: &str) -> String {
    if value.is_null() {
        return String::new();
    }
    if let Value::Array(values) = value {
        let item = match key {
            "QueueUrls" | "queueUrls" => "QueueUrl".into(),
            "Messages" => "Message".into(),
            "Successful" => format!("{action}ResultEntry"),
            "Failed" => "BatchResultErrorEntry".into(),
            "Results" if action == "ListMessageMoveTasks" => {
                "ListMessageMoveTasksResultEntry".into()
            }
            _ => key.into(),
        };
        return values
            .iter()
            .map(|value| xml_field(&item, value, action))
            .collect();
    }
    if let Value::Object(values) = value {
        if matches!(key, "Attributes" | "Tags" | "MessageAttributes") {
            let item = match key {
                "Tags" => "Tag",
                "MessageAttributes" => "MessageAttribute",
                _ => "Attribute",
            };
            let name = if key == "Tags" { "Key" } else { "Name" };
            return values
                .iter()
                .map(|(k, v)| {
                    format!(
                        "<{item}><{name}>{}</{name}>{}</{item}>",
                        escape(k),
                        xml_field("Value", v, action)
                    )
                })
                .collect();
        }
        let content: String = values
            .iter()
            .map(|(k, v)| xml_field(k, v, action))
            .collect();
        return format!("<{key}>{content}</{key}>");
    }
    let text = value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string());
    format!("<{key}>{}</{key}>", escape(&text))
}

fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\r', "&#13;")
}
