use std::{
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use tokio::io::AsyncWriteExt;
use tonic::{Status, Streaming};
use uuid::Uuid;

use crate::{
    android::MediaFile,
    error::ArisaError,
    proto::{self, MediaMode, SendMediaByChunkRequest, send_media_by_chunk_request::Payload},
};

const DEFAULT_MIME: &str = "application/octet-stream";
const RETENTION: Duration = Duration::from_secs(600);

pub struct StoredMedia {
    pub channel_id: i64,
    pub files: Vec<MediaFile>,
    pub multiple: bool,
}

struct CurrentFile {
    file: tokio::fs::File,
    expected_size: u64,
    received_size: u64,
}

pub async fn store_chunks(
    directory: &Path,
    mut stream: Streaming<SendMediaByChunkRequest>,
) -> Result<StoredMedia, Status> {
    let mut stored = Vec::new();
    let result = store_chunks_inner(directory, &mut stream, &mut stored).await;
    if result.is_err() {
        remove_stored(&stored).await;
    }
    result
}

async fn store_chunks_inner(
    directory: &Path,
    stream: &mut Streaming<SendMediaByChunkRequest>,
    stored: &mut Vec<MediaFile>,
) -> Result<StoredMedia, Status> {
    let first = stream
        .message()
        .await?
        .ok_or_else(|| Status::invalid_argument("metadata is required"))?;
    let metadata = match first.payload {
        Some(Payload::Metadata(metadata)) => metadata,
        _ => {
            return Err(Status::invalid_argument(
                "the first message must be metadata",
            ));
        }
    };
    if metadata.file_count == 0 {
        return Err(Status::invalid_argument(
            "at least one media file is required",
        ));
    }
    let mode = MediaMode::try_from(metadata.mode)
        .map_err(|_| Status::invalid_argument("invalid media mode"))?;
    let expected_count = metadata.file_count as usize;
    let mut current: Option<CurrentFile> = None;

    while let Some(message) = stream.message().await? {
        match message.payload {
            Some(Payload::Metadata(_)) => {
                return Err(Status::invalid_argument("metadata can only be sent once"));
            }
            Some(Payload::FileMetadata(file_metadata)) => {
                if current.is_some() {
                    return Err(Status::invalid_argument(
                        "the current media file is incomplete",
                    ));
                }
                if stored.len() >= expected_count {
                    return Err(Status::invalid_argument("file count exceeds metadata"));
                }
                if file_metadata.file_size == 0 {
                    return Err(Status::invalid_argument("media files cannot be empty"));
                }
                let path = directory.join(Uuid::new_v4().to_string());
                let file = tokio::fs::File::create(&path).await.map_err(|error| {
                    Status::internal(format!("failed to store media file: {error}"))
                })?;
                stored.push(MediaFile {
                    path: path.to_string_lossy().into_owned(),
                    mime: if file_metadata.content_type.trim().is_empty() {
                        DEFAULT_MIME.to_string()
                    } else {
                        file_metadata.content_type
                    },
                    name: (!file_metadata.file_name.is_empty()).then_some(file_metadata.file_name),
                });
                current = Some(CurrentFile {
                    file,
                    expected_size: file_metadata.file_size,
                    received_size: 0,
                });
            }
            Some(Payload::Chunk(chunk)) => {
                let file = current
                    .as_mut()
                    .ok_or_else(|| Status::invalid_argument("file metadata must precede chunks"))?;
                let chunk_size = u64::try_from(chunk.len())
                    .map_err(|_| Status::invalid_argument("media chunk is too large"))?;
                let received_size = file
                    .received_size
                    .checked_add(chunk_size)
                    .ok_or_else(|| Status::invalid_argument("media file size overflow"))?;
                if received_size > file.expected_size {
                    return Err(Status::invalid_argument(
                        "media file exceeds its declared size",
                    ));
                }
                file.file.write_all(&chunk).await.map_err(|error| {
                    Status::internal(format!("failed to store media file: {error}"))
                })?;
                file.received_size = received_size;
                if received_size == file.expected_size {
                    current = None;
                }
            }
            None => return Err(Status::invalid_argument("request payload is required")),
        }
    }

    if current.is_some() {
        return Err(Status::invalid_argument(
            "the final media file is incomplete",
        ));
    }
    if stored.len() != expected_count {
        return Err(Status::invalid_argument(
            "file count does not match metadata",
        ));
    }

    Ok(StoredMedia {
        channel_id: metadata.channel_id,
        files: std::mem::take(stored),
        multiple: mode == MediaMode::Multiple,
    })
}

pub async fn store(
    directory: &Path,
    files: Vec<proto::MediaFile>,
) -> Result<Vec<MediaFile>, ArisaError> {
    if files.is_empty() {
        return Err(ArisaError::InvalidArgument(
            "at least one media file is required".to_string(),
        ));
    }

    let mut stored = Vec::with_capacity(files.len());
    for file in files {
        if file.data.is_empty() {
            remove_stored(&stored).await;
            return Err(ArisaError::InvalidArgument(
                "media files cannot be empty".to_string(),
            ));
        }
        let path = directory.join(Uuid::new_v4().to_string());
        if let Err(error) = tokio::fs::write(&path, file.data).await {
            remove_stored(&stored).await;
            return Err(ArisaError::Internal(format!(
                "failed to store media file: {error}"
            )));
        }
        stored.push(MediaFile {
            path: path.to_string_lossy().into_owned(),
            mime: if file.mime.trim().is_empty() {
                DEFAULT_MIME.to_string()
            } else {
                file.mime
            },
            name: file.name,
        });
    }
    Ok(stored)
}

pub fn start_cleanup(directory: PathBuf) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(RETENTION);
        loop {
            interval.tick().await;
            cleanup(&directory).await;
        }
    });
}

async fn cleanup(directory: &Path) {
    let cutoff = SystemTime::now()
        .checked_sub(RETENTION)
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let Ok(mut entries) = tokio::fs::read_dir(directory).await else {
        return;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let Ok(metadata) = entry.metadata().await else {
            continue;
        };
        if metadata.modified().is_ok_and(|modified| modified < cutoff) {
            let _ = tokio::fs::remove_file(entry.path()).await;
        }
    }
}

async fn remove_stored(files: &[MediaFile]) {
    for file in files {
        let _ = tokio::fs::remove_file(&file.path).await;
    }
}
