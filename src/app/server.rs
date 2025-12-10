// a simple server that can handle requests from the client to the database
// endpoints:
// GET /key (returns the complete key-value pair)
// POST /key (which will set/update the key-value pair)
// DELETE /key (which will soft delete [it is marked tombstone, deletes when compaction] the key-value pair)

use crate::app::types::{Payload, PostPayload, Response};
use crate::engine::lsm::LsmTree;
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{delete, get, post},
};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
// Arc is an atomic reference-counted pointer type from the standard library,
// which enables safe shared ownership of values across multiple threads.
// It's commonly used to share immutable or thread-safe data (such as with Mutex or RwLock)
// in asynchronous or multi-threaded Rust applications.
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info};

const DATA_DIR_ENV: &str = "SNAILDB_DATA_DIR";
const DEFAULT_DATA_DIR: &str = "data";

#[derive(Clone)]
struct AppState {
    store: Arc<Mutex<LsmTree>>, // the database store
}

impl AppState {
    fn new(store: LsmTree) -> Self {
        Self {
            store: Arc::new(Mutex::new(store)),
        }
    }
}

pub fn build_router() -> Router {
    // method to build and return the router when called from main
    let state = initialize_state();
    Router::new()
        .route("/key", get(get_key))
        .route("/key", post(set_key))
        .route("/key", delete(delete_key))
        .with_state(state)
}

fn initialize_state() -> AppState {
    let data_dir = std::env::var(DATA_DIR_ENV).unwrap_or_else(|_| DEFAULT_DATA_DIR.to_string());
    let store = LsmTree::open(&data_dir).unwrap_or_else(|err| {
        panic!("failed to initialize LSM tree at {}: {}", data_dir, err);
    });
    AppState::new(store)
}

async fn get_key(
    State(state): State<AppState>,
    Json(payload): Json<Payload>,
) -> Result<Json<Response>, StatusCode> {
    // method to get the key-value pair from the database
    let key = payload.key;
    info!(%key, "GET /key request received");
    let store = state.store.lock().await;
    let value = store.get(&key).map_err(|err| {
        error!(%key, error = %err, "failed to read key from store");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    drop(store);

    let response = match value {
        Some(bytes) => {
            let decoded = decode_value(&bytes);
            info!(%key, "GET /key hit");
            Response {
                status: "success".to_string(),
                message: format!("Key {} fetched successfully", key),
                data: Some(HashMap::from([(key.clone(), decoded)])),
            }
        }
        None => {
            info!(%key, "GET /key miss");
            Response {
                status: "error".to_string(),
                message: format!("Key {} not found", key),
                data: None,
            }
        }
    };

    Ok(Json(response))
}

async fn set_key(
    State(state): State<AppState>,
    Json(payload): Json<PostPayload>,
) -> Result<Json<Response>, StatusCode> {
    // method to set the key-value pair in the database
    let key = payload.key;
    let value = payload.value;
    let mut store = state.store.lock().await;
    let encoded_value = serde_json::to_vec(&value).map_err(|err| {
        error!(%key, error = %err, "failed to serialize value");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    store.put(&key, encoded_value).map_err(|err| {
        error!(%key, error = %err, "failed to set key");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    drop(store);

    Ok(Json(Response {
        status: "success".to_string(),
        message: format!("Key {} set successfully", key),
        data: Some(HashMap::from([(key, value)])),
    }))
}

async fn delete_key(
    State(state): State<AppState>,
    Json(payload): Json<Payload>,
) -> Result<Json<Response>, StatusCode> {
    // method to soft delete the key-value pair in the database
    let key = payload.key;
    let mut store = state.store.lock().await;
    store.delete(&key).map_err(|err| {
        error!(%key, error = %err, "failed to delete key");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    drop(store);

    Ok(Json(Response {
        status: "success".to_string(),
        message: format!("Key {} deleted", key),
        data: None,
    }))
}

fn decode_value(bytes: &[u8]) -> JsonValue {
    serde_json::from_slice(bytes).unwrap_or_else(|_| {
        let fallback = String::from_utf8_lossy(bytes).into_owned();
        JsonValue::String(fallback)
    })
}
