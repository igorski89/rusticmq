use rusticmq::{
    config::AppConfig,
    persistence::Persistence,
    sqs::{SqsService, now},
};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut config_path = std::env::var_os("RUSTICMQ_CONFIG").map(PathBuf::from);
    let mut data_dir = std::env::var_os("RUSTICMQ_DATA_DIR").map(PathBuf::from);
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(
                    args.next().ok_or("--config requires a path")?,
                ))
            }
            "--data-dir" => {
                data_dir = Some(PathBuf::from(
                    args.next()
                        .ok_or("--data-dir requires a directory or :memory:")?,
                ))
            }
            "--help" | "-h" => {
                println!("rusticmq [--config elasticmq.conf] [--data-dir :memory:|DIR]");
                return Ok(());
            }
            _ => return Err(format!("Unknown argument: {arg}").into()),
        }
    }
    let mut app = AppConfig::load(config_path.as_deref(), now())?;
    if let Some(dir) = data_dir {
        app.message_storage = Some(database_in(dir)?);
    }
    if let Ok(value) = std::env::var("RUSTICMQ_BIND") {
        app.bind = value;
    }
    if let Ok(value) = std::env::var("RUSTICMQ_QUEUES_STORAGE") {
        app.queue_storage = Some(PathBuf::from(value));
    }
    let mut config = app.service;
    if let Ok(value) = std::env::var("RUSTICMQ_BASE_URL") {
        config.base_url = value;
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
    if !app.enabled {
        shutdown_signal().await;
        return Ok(());
    }
    let listener = tokio::net::TcpListener::bind(&app.bind).await?;
    if app.generate_node_address {
        config.base_url = format!("http://{}{}", listener.local_addr()?, config.context_path);
    }
    eprintln!("RusticMQ listening on {}", listener.local_addr()?);
    let service = SqsService::new(manager, config).with_persistence(persistence);
    axum::serve(listener, service.clone().router())
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            service.shutdown();
        })
        .await?;
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
    if dir.as_os_str() == ":memory:" || dir.as_os_str() == ":memory" {
        return Ok(PathBuf::from(":memory:"));
    }
    std::fs::create_dir_all(&dir)?;
    Ok(dir.join("rusticmq.sqlite"))
}
