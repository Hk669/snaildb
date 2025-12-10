use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

#[derive(Deserialize)]
pub struct Payload {
    pub key: String,
}

#[derive(Deserialize)]
pub struct PostPayload {
    pub key: String,
    pub value: JsonValue,
}

#[derive(Serialize)]
pub struct Response {
    pub status: String,
    pub message: String,
    pub data: Option<HashMap<String, JsonValue>>,
}
