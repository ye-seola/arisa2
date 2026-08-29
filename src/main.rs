mod android;
mod config;
mod credential;
mod database;
mod error;
mod media;
mod proto;
mod service;

use std::{error::Error, io::Read};

use tokio::sync::broadcast;
use tonic::transport::Server;

use crate::{
    android::{ActionProcessor, create_android_vm},
    config::MAX_GRPC_MESSAGE_BYTES,
    database::{Database, create_pool, query_current_user_id, start_poller},
    proto::arisa_server::ArisaServer,
    service::ArisaService,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!(
        "Arisa v{} ({})",
        env!("CARGO_PKG_VERSION"),
        env!("ARISA_COMMIT_SHA")
    );

    let config = config::load();
    tokio::fs::create_dir_all(&config.temp_dir).await?;

    let pool = create_pool(&config.app_path, &config.database_key);
    let current_user_id = query_current_user_id(&pool);
    let database = Database::new(pool, current_user_id);
    let (events, _) = broadcast::channel(128);
    start_poller(database.clone(), events.clone(), config.db_pull_delay);

    let jvm = unsafe { create_android_vm() }
        .map_err(|error| format!("failed to create Android VM: {error}"))?;
    let actions = ActionProcessor::spawn(jvm, config.uid, config.calling_package, config.referer);
    let service = ArisaService::new(
        database,
        actions,
        events,
        config.app_path,
        config.temp_dir.clone(),
    );
    media::start_cleanup(config.temp_dir);

    let address = config.bind.parse()?;
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .build_v1()?;
    println!("gRPC listening on {address}");
    Server::builder()
        .add_service(reflection)
        .add_service(
            ArisaServer::new(service)
                .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_GRPC_MESSAGE_BYTES),
        )
        .serve_with_shutdown(address, shutdown_signal(config.exit_on_stdin_close))
        .await?;
    Ok(())
}

async fn shutdown_signal(exit_on_stdin_close: bool) {
    if exit_on_stdin_close {
        stdin_closed().await;
    } else {
        std::future::pending::<()>().await;
    }
}

async fn stdin_closed() {
    let _ = tokio::task::spawn_blocking(|| {
        let mut stdin = std::io::stdin().lock();
        let mut byte = [0];
        while stdin.read(&mut byte).is_ok_and(|read| read > 0) {}
    })
    .await;
}
