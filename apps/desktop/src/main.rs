//! Tauri shell around the in-process daemon. The webview calls an `api` command instead of
//! the socket, so the bearer token never reaches it.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use qd_core::server::Policy;
use qd_daemon::{Daemon, DaemonConfig, DaemonEvent};
use serde::Serialize;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tauri::Emitter;
use tauri_plugin_dialog::DialogExt;

struct AppState {
    daemon: Arc<Daemon>,
    http: reqwest::Client,
}

#[derive(Serialize)]
struct DaemonInfo {
    name: String,
    device_id: String,
    short_id: String,
    api: String,
    quic_port: u16,
}

async fn do_api(
    state: &AppState,
    method: &str,
    path: &str,
    body: Option<Value>,
) -> Result<Value, String> {
    let url = format!("http://{}{}", state.daemon.addr, path);
    let request = match method.to_ascii_uppercase().as_str() {
        "GET" => state.http.get(url),
        "POST" => state.http.post(url),
        "PATCH" => state.http.patch(url),
        "DELETE" => state.http.delete(url),
        other => return Err(format!("unsupported method {other}")),
    };
    let request = match body {
        Some(body) => request.json(&body),
        None => request,
    };
    let response = request
        .bearer_auth(&state.daemon.token)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    let status = response.status();
    let text = response.text().await.map_err(|e| e.to_string())?;
    let value: Value = if text.trim().is_empty() {
        Value::Null
    } else {
        serde_json::from_str(&text).unwrap_or_else(|_| json!({ "error": text }))
    };
    if status.is_success() {
        Ok(value)
    } else {
        Err(value
            .get("error")
            .and_then(|e| e.as_str())
            .unwrap_or("request failed")
            .to_string())
    }
}

#[tauri::command]
async fn api(
    state: tauri::State<'_, AppState>,
    method: String,
    path: String,
    body: Option<Value>,
) -> Result<Value, String> {
    do_api(&state, &method, &path, body).await
}

#[tauri::command]
async fn daemon_info(state: tauri::State<'_, AppState>) -> Result<DaemonInfo, String> {
    let status: Value = do_api(&state, "GET", "/api/status", None).await?;
    Ok(DaemonInfo {
        name: status["name"].as_str().unwrap_or("").to_string(),
        device_id: status["device_id"].as_str().unwrap_or("").to_string(),
        short_id: status["short_id"].as_str().unwrap_or("").to_string(),
        api: status["api"].as_str().unwrap_or("").to_string(),
        quic_port: status["quic_port"].as_u64().unwrap_or(0) as u16,
    })
}

#[tauri::command]
async fn pick_files(app: tauri::AppHandle) -> Result<Vec<String>, String> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    app.dialog().file().pick_files(move |paths| {
        let _ = tx.send(paths);
    });
    let paths = rx.await.map_err(|e| e.to_string())?;
    Ok(paths
        .unwrap_or_default()
        .into_iter()
        .filter_map(|path| path.into_path().ok())
        .map(|path| path.display().to_string())
        .collect())
}

#[tauri::command]
async fn open_path(path: String) -> Result<(), String> {
    let opener = if cfg!(target_os = "macos") {
        "open"
    } else if cfg!(target_os = "windows") {
        "explorer"
    } else {
        "xdg-open"
    };
    // Forking is blocking work; keep it off the runtime.
    tokio::task::spawn_blocking(move || {
        std::process::Command::new(opener)
            .arg(&path)
            .spawn()
            .map(|_| ())
            .map_err(|e| format!("cannot open {path}: {e}"))
    })
    .await
    .map_err(|e| e.to_string())?
}

#[tauri::command]
async fn pick_folders(app: tauri::AppHandle) -> Result<Vec<String>, String> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    app.dialog().file().pick_folders(move |paths| {
        let _ = tx.send(paths);
    });
    let paths = rx.await.map_err(|e| e.to_string())?;
    Ok(paths
        .unwrap_or_default()
        .into_iter()
        .filter_map(|p| p.into_path().ok())
        .map(|p| p.display().to_string())
        .collect())
}

#[tauri::command]
async fn pick_folder(app: tauri::AppHandle) -> Result<Option<String>, String> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    app.dialog().file().pick_folder(move |path| {
        let _ = tx.send(path);
    });
    let path = rx.await.map_err(|e| e.to_string())?;
    Ok(path
        .and_then(|p| p.into_path().ok())
        .map(|p| p.display().to_string()))
}

/// Events that arrive in bursts and are delivered to the webview a few at a time.
fn is_burst(event: &DaemonEvent) -> bool {
    matches!(
        event,
        DaemonEvent::Progress { .. } | DaemonEvent::File { .. }
    )
}

fn flush(handle: &tauri::AppHandle, pending: &mut Vec<DaemonEvent>) {
    match pending.len() {
        0 => {}
        1 => {
            let _ = handle.emit("daemon-event", &pending[0]);
            pending.clear();
        }
        _ => {
            let batch = std::mem::take(pending);
            let _ = handle.emit("daemon-batch", &batch);
        }
    }
}

fn main() {
    // The daemon needs a real runtime; Tauri's event loop is not async. Keep the
    // runtime alive for the whole process and hand its handle to Tauri.
    let runtime = Box::leak(Box::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime"),
    ));

    let cfg = DaemonConfig {
        policy: Policy::Ask,
        ..Default::default()
    };
    let daemon = runtime
        .block_on(Daemon::start(cfg))
        .expect("start quicdrop daemon");
    eprintln!(
        "quicdrop desktop: api {} (token in daemon.json)",
        daemon.addr
    );

    tauri::async_runtime::set(runtime.handle().clone());

    let daemon = Arc::new(daemon);
    let state_daemon = daemon.clone();
    let setup_daemon = daemon.clone();

    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .manage(AppState {
            daemon: state_daemon,
            http: reqwest::Client::new(),
        })
        .setup(move |app| {
            // Forward daemon events to the webview. Progress and per-file events are
            // coalesced per tick; each one is a serialize plus an IPC round trip.
            let handle = app.handle().clone();
            let mut events = setup_daemon.state().subscribe();
            tauri::async_runtime::spawn(async move {
                let mut pending: Vec<DaemonEvent> = Vec::new();
                let mut tick = tokio::time::interval(Duration::from_millis(100));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        received = events.recv() => match received {
                            Ok(event) => {
                                if is_burst(&event) {
                                    pending.push(event);
                                } else {
                                    flush(&handle, &mut pending);
                                    let _ = handle.emit("daemon-event", &event);
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                // A dropped session or approval leaves the window stale.
                                eprintln!("event stream lagged by {n}, asking the ui to resync");
                                pending.clear();
                                let _ = handle.emit("daemon-resync", ());
                            }
                            Err(_) => break,
                        },
                        _ = tick.tick() => flush(&handle, &mut pending),
                    }
                }
            });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            api,
            daemon_info,
            pick_files,
            pick_folder,
            pick_folders,
            open_path
        ])
        .build(tauri::generate_context!())
        .expect("build tauri app")
        .run(move |_app, event| {
            if let tauri::RunEvent::ExitRequested { .. } = event {
                daemon.shutdown();
            }
        });
}
