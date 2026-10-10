use crate::{AttributeValue, Attributes};
use md5::{Digest, Md5};

pub fn body_digest(body: &str) -> String {
    format!("{:x}", Md5::digest(body.as_bytes()))
}

pub fn attribute_digest(attributes: &Attributes) -> String {
    let mut bytes = Vec::new();
    for (name, attribute) in attributes {
        encoded(&mut bytes, name.as_bytes());
        encoded(&mut bytes, attribute.data_type().as_bytes());
        match &attribute.value {
            AttributeValue::String(value) | AttributeValue::Number(value) => {
                bytes.push(1);
                encoded(&mut bytes, value.as_bytes());
            }
            AttributeValue::Binary(value) => {
                bytes.push(2);
                encoded(&mut bytes, value);
            }
        }
    }
    format!("{:x}", Md5::digest(&bytes))
}

fn encoded(bytes: &mut Vec<u8>, value: &[u8]) {
    bytes.extend((value.len() as u32).to_be_bytes());
    bytes.extend(value);
}
