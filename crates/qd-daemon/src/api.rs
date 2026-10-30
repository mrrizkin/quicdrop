//! Local JSON + SSE API.

use crate::{spawn_send, DaemonEvent, Inner, PeerView, TransferStatus, TransferView};
use axum::extract::{Path, Query, Request, State};
use axum::http::{header, StatusCode};
use axum::middleware::{self, Next};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use qd_core::approval::Decision;
use qd_core::server::Policy;
use qd_core::Identity;
use serde::Deserialize;
use serde_json::{json, Value};
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};
use tracing::warn;

pub fn router(inner: Arc<Inner>) -> Router {
    Router::new()
        .route("/api/status", get(status))
        .route("/api/peers", get(peers))
        .route("/api/transfers", get(transfers))
        .route("/api/approvals", get(approvals))
        .route("/api/approvals/{id}", post(respond_approval))
        .route("/api/trust", get(trust_list).post(trust_add))
        .route("/api/trust/{id}", delete(trust_remove))
        .route("/api/send", post(send))
        .route("/api/config", get(config).patch(patch_config))
        .route("/api/events", get(events))
        .route("/api/shutdown", post(shutdown))
        .layer(middleware::from_fn_with_state(inner.clone(), auth))
        .with_state(inner)
}

/// Every endpoint requires the bearer token, except for `?token=` on the event
/// stream (`EventSource` cannot set headers in browsers).
async fn auth(State(inner): State<Arc<Inner>>, req: Request, next: Next) -> Response {
    let header_ok = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(|token| token == inner.token)
        .unwrap_or(false);
    let query_ok = req
        .uri()
        .query()
        .map(|query| {
            query
                .split('&')
                .any(|pair| pair.strip_prefix("token=") == Some(inner.token.as_str()))
        })
        .unwrap_or(false);

    if header_ok || query_ok {
        next.run(req).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "missing or invalid api token" })),
        )
            .into_response()
    }
}

struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

fn bad_request(message: impl Into<String>) -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, message.into())
}

fn not_found(message: impl Into<String>) -> ApiError {
    ApiError(StatusCode::NOT_FOUND, message.into())
}

#[derive(Deserialize)]
struct Respond {
    accept: bool,
    #[serde(default)]
    remember: bool,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Deserialize)]
struct SendRequest {
    /// Peer name, device id prefix, `ip:port`, or an address.
    to: String,
    paths: Vec<PathBuf>,
    #[serde(default)]
    concurrency: Option<usize>,
    #[serde(default)]
    timeout_secs: Option<u64>,
}

#[derive(Deserialize)]
struct TrustAdd {
    device_id: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    platform: Option<String>,
}

#[derive(Deserialize)]
struct ConfigPatch {
    #[serde(default)]
    out_dir: Option<PathBuf>,
    #[serde(default)]
    policy: Option<Policy>,
}

#[derive(Deserialize)]
struct Limit {
    #[serde(default)]
    limit: Option<usize>,
}

async fn status(State(inner): State<Arc<Inner>>) -> Json<Value> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let transfers = inner.transfers(200);
    let active = transfers
        .iter()
        .filter(|t| t.status == TransferStatus::Active)
        .count();
    Json(json!({
        "name": inner.identity.name,
        "device_id": inner.identity.device_id,
        "short_id": inner.identity.short_id(),
        "platform": Identity::platform(),
        "version": inner.version,
        "api": inner.listen.to_string(),
        "quic_port": inner.quic_port,
        "discovery_port": inner.discovery_port,
        "local_addrs": inner.local_addrs,
        "out_dir": inner.runtime.out_dir(),
        "policy": inner.runtime.policy(),
        "uptime_secs": now.saturating_sub(inner.started_at),
        "peers": inner.peers().len(),
        "trusted": inner.trusted_peers().len(),
        "pending_approvals": inner.pending_approvals().len(),
        "active_transfers": active,
        "event_subscribers": inner.event_capacity(),
    }))
}

async fn peers(State(inner): State<Arc<Inner>>) -> Json<Vec<PeerView>> {
    Json(inner.peers())
}

async fn transfers(State(inner): State<Arc<Inner>>, Query(q): Query<Limit>) -> Json<Vec<TransferView>> {
    Json(inner.transfers(q.limit.unwrap_or(50).clamp(1, 200)))
}

async fn approvals(State(inner): State<Arc<Inner>>) -> Json<Vec<crate::ApprovalView>> {
    Json(inner.pending_approvals())
}

async fn respond_approval(
    State(inner): State<Arc<Inner>>,
    Path(id): Path<String>,
    Json(body): Json<Respond>,
) -> Result<Json<Value>, ApiError> {
    let view = inner
        .pending_approvals()
        .into_iter()
        .find(|view| view.id == id)
        .ok_or_else(|| not_found(format!("no pending approval {id}")))?;

    let decision = if body.accept {
        if body.remember {
            inner
                .trust
                .remember(&view.peer_id, &view.peer_name, &view.peer_platform)
                .map_err(|e| {
                    ApiError(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("could not save trust: {e:#}"),
                    )
                })?;
            inner.broadcast(DaemonEvent::Trust {
                peers: inner.trusted_peers(),
            });
            Decision::accept_always()
        } else {
            Decision::accept()
        }
    } else {
        Decision::reject(body.reason.unwrap_or_else(|| "declined by receiver".into()))
    };

    if !inner.resolve_approval(&id, decision) {
        return Err(not_found(format!("approval {id} already resolved")));
    }
    Ok(Json(json!({ "ok": true })))
}

async fn trust_list(State(inner): State<Arc<Inner>>) -> Json<Vec<qd_core::trust::TrustedPeer>> {
    Json(inner.trusted_peers())
}

async fn trust_add(
    State(inner): State<Arc<Inner>>,
    Json(body): Json<TrustAdd>,
) -> Result<Json<Value>, ApiError> {
    let name = body
        .name
        .unwrap_or_else(|| format!("device {}", &body.device_id[..8.min(body.device_id.len())]));
    inner
        .trust
        .remember(
            &body.device_id,
            &name,
            body.platform.as_deref().unwrap_or(""),
        )
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;
    inner.broadcast(DaemonEvent::Trust {
        peers: inner.trusted_peers(),
    });
    Ok(Json(json!({ "ok": true, "device_id": body.device_id })))
}

async fn trust_remove(
    State(inner): State<Arc<Inner>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let removed = inner
        .trust
        .forget_prefix(&id)
        .map_err(|e| bad_request(format!("{e:#}")))?;
    let peer = removed.ok_or_else(|| not_found(format!("no trusted device matches {id}")))?;
    inner.broadcast(DaemonEvent::Trust {
        peers: inner.trusted_peers(),
    });
    Ok(Json(json!({ "ok": true, "device_id": peer.device_id })))
}

async fn send(
    State(inner): State<Arc<Inner>>,
    Json(body): Json<SendRequest>,
) -> Result<Json<Value>, ApiError> {
    if body.paths.is_empty() {
        return Err(bad_request("no paths given"));
    }
    for path in &body.paths {
        match std::fs::metadata(path) {
            Ok(meta) if meta.is_file() => {}
            Ok(_) => return Err(bad_request(format!("not a file: {}", path.display()))),
            Err(e) => {
                return Err(bad_request(format!("cannot read {}: {e}", path.display())))
            }
        }
    }
    let id = spawn_send(
        inner,
        body.to,
        body.paths,
        body.concurrency.unwrap_or(4),
        Duration::from_secs(body.timeout_secs.unwrap_or(5).max(1)),
    );
    Ok(Json(json!({ "id": id })))
}

async fn config(State(inner): State<Arc<Inner>>) -> Json<Value> {
    Json(json!({
        "out_dir": inner.runtime.out_dir(),
        "policy": inner.runtime.policy(),
    }))
}

async fn patch_config(
    State(inner): State<Arc<Inner>>,
    Json(body): Json<ConfigPatch>,
) -> Result<Json<Value>, ApiError> {
    if let Some(dir) = &body.out_dir {
        std::fs::create_dir_all(dir)
            .map_err(|e| bad_request(format!("cannot create {}: {e}", dir.display())))?;
        inner.runtime.set_out_dir(dir.clone());
    }
    if let Some(policy) = body.policy {
        inner.runtime.set_policy(policy);
    }
    inner.broadcast(DaemonEvent::Config {
        out_dir: inner.runtime.out_dir().display().to_string(),
        policy: inner.runtime.policy(),
    });
    Ok(Json(json!({
        "out_dir": inner.runtime.out_dir(),
        "policy": inner.runtime.policy(),
    })))
}

async fn shutdown(State(inner): State<Arc<Inner>>) -> Json<Value> {
    let _ = inner.shutdown_signal();
    Json(json!({ "ok": true }))
}

async fn events(
    State(inner): State<Arc<Inner>>,
) -> Sse<impl Stream<Item = Result<SseEvent, Infallible>>> {
    let hello = DaemonEvent::Hello {
        name: inner.identity.name.clone(),
        device_id: inner.identity.device_id.clone(),
        peers: inner.peers(),
        approvals: inner.pending_approvals(),
        active: inner
            .transfers(50)
            .into_iter()
            .filter(|t| t.status == TransferStatus::Active)
            .collect(),
    };
    let first = tokio_stream::once(Ok::<_, Infallible>(sse(&hello)));

    let rx = inner.subscribe();
    let rest = BroadcastStream::new(rx).filter_map(|item| match item {
        Ok(event) => Some(Ok(sse(&event))),
        Err(e) => {
            warn!("event stream lagged: {e}");
            None
        }
    });

    Sse::new(first.chain(rest)).keep_alive(KeepAlive::default())
}

fn sse(event: &DaemonEvent) -> SseEvent {
    let name = match event {
        DaemonEvent::Hello { .. } => "hello",
        DaemonEvent::Peer { .. } => "peer",
        DaemonEvent::Session { .. } => "session",
        DaemonEvent::Progress { .. } => "progress",
        DaemonEvent::Approval { .. } => "approval",
        DaemonEvent::ApprovalResolved { .. } => "approval_resolved",
        DaemonEvent::Trust { .. } => "trust",
        DaemonEvent::Config { .. } => "config",
    };
    SseEvent::default()
        .event(name)
        .json_data(event)
        .unwrap_or_else(|_| SseEvent::default().event("error").data("{}"))
}
