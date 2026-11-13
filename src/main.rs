use rusticmq::{
    config::AppConfig,
    persistence::Persistence,
    sqs::{SqsService, Storage, now},
};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut config_path = std::env::var_os("RUSTICMQ_CONFIG").map(PathBuf::from);
    let mut data_dir = std::env::var_os("RUSTICMQ_DATA_DIR").map(PathBuf::from);
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        // Both `--option value` and `--option=value`.
        let (name, inline) = match arg.split_once('=') {
            Some((name, value)) if name.starts_with("--") => (name, Some(value.to_owned())),
            _ => (arg.as_str(), None),
        };
        let mut value = |error: &'static str| inline.clone().or_else(|| args.next()).ok_or(error);
        match name {
            "--config" => config_path = Some(PathBuf::from(value("--config requires a path")?)),
            "--data-dir" => {
                data_dir = Some(PathBuf::from(value(
                    "--data-dir requires a directory or :memory:",
                )?))
            }
            "--help" | "-h" => {
                println!("rusticmq [--config elasticmq.conf] [--data-dir DIR|:memory:]");
                return Ok(());
            }
            _ => return Err(format!("Unknown argument: {arg}").into()),
        }
    }
    let mut app = AppConfig::load(config_path.as_deref(), now())?;
    if let Some(dir) = data_dir {
        app.message_storage = Some(database_in(dir)?);
    }
    app.service.configured_queues = app.queues.len();
    app.service.storage = match &app.message_storage {
        None => Storage::Memory,
        Some(path) if is_memory(path) => Storage::SqliteMemory,
        Some(_) => Storage::Sqlite,
    };
    if let Ok(value) = std::env::var("RUSTICMQ_BIND") {
        app.bind = value;
    }
    if let Ok(value) = std::env::var("RUSTICMQ_QUEUES_STORAGE") {
        app.queue_storage = Some(PathBuf::from(value));
    }
    let mut config = app.service;
    // A fixed base URL replaces the default of following each request's host.
    if let Ok(value) = std::env::var("RUSTICMQ_BASE_URL") {
        config.base_url = value;
        config.wildcard_queue_urls = false;
    }
    if let Ok(value) = std::env::var("RUSTICMQ_REGION") {
        config.region = value;
    }
    if let Ok(value) = std::env::var("RUSTICMQ_ACCOUNT_ID") {
        config.account_id = value;
    }
    let persistence =
        Persistence::open(app.queue_storage, app.message_storage, app.prune_messages)?;
    let mut manager = persistence.restore_with_limits(app.queues, now(), config.relaxed_limits)?;
    persistence.apply(&manager.drain_events(), &manager)?;
    // With no database nothing reads message events, so don't build them.
    manager.set_discard_message_events(!persistence.has_database());
    if !app.enabled {
        shutdown_signal().await;
        return Ok(());
    }
    let listener = tokio::net::TcpListener::bind(&app.bind).await?;
    if app.generate_node_address {
        config.base_url = format!("http://{}{}", listener.local_addr()?, config.context_path);
        config.wildcard_queue_urls = false;
    }
    eprintln!("RusticMQ listening on {}", listener.local_addr()?);
    let service = SqsService::new(manager, config).with_persistence(persistence);
    let stopping = service.clone();
    axum::serve(listener, service.clone().router())
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            stopping.shutdown();
        })
        .await?;
    if let Err(error) = service.flush().await {
        eprintln!("Persistence failed during shutdown: {error}");
    }
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// SQLite in WAL mode keeps `-wal` and `-shm` files beside the database, so
/// storage is configured as a directory rather than a single file.
fn database_in(dir: PathBuf) -> std::io::Result<PathBuf> {
    if is_memory(&dir) {
        return Ok(PathBuf::from(":memory:"));
    }
    std::fs::create_dir_all(&dir)?;
    Ok(dir.join("rusticmq.sqlite"))
}

/// SQLite's in-memory database name, also accepted without the final colon.
fn is_memory(path: &std::path::Path) -> bool {
    path.as_os_str() == ":memory:" || path.as_os_str() == ":memory"
}
