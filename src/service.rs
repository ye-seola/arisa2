use std::{path::PathBuf, pin::Pin};

use tokio::sync::broadcast;
use tokio_stream::{Stream, StreamExt, wrappers::BroadcastStream};
use tonic::{Request, Response, Status};

use crate::{
    android::{Action, ActionProcessor},
    credential,
    database::Database,
    media,
    proto::{
        Channel, ChannelMembers, Credential, DecryptRequest, DecryptResponse, EnterChannelRequest,
        Event, GetChannelMembersRequest, GetChannelRequest, GetMessageRequest, GetMessagesRequest,
        GetMessagesResponse, GetUserRequest, GetUsersRequest, GetUsersResponse, MarkReadRequest,
        MediaMode, Member, RawQueryRequest, RawQueryResponse, ReplyRequest,
        SendMediaByChunkRequest, SendMediaRequest, SubscribeEventsRequest, arisa_server::Arisa,
    },
};

#[derive(Clone)]
pub struct ArisaService {
    database: Database,
    actions: ActionProcessor,
    events: broadcast::Sender<Event>,
    app_path: String,
    temp_dir: PathBuf,
}

impl ArisaService {
    pub fn new(
        database: Database,
        actions: ActionProcessor,
        events: broadcast::Sender<Event>,
        app_path: String,
        temp_dir: PathBuf,
    ) -> Self {
        Self {
            database,
            actions,
            events,
            app_path,
            temp_dir,
        }
    }

    fn enqueue(&self, action: Action) -> Result<Response<()>, Status> {
        self.actions.enqueue(action).map_err(Status::unavailable)?;
        Ok(Response::new(()))
    }
}

type EventStream = Pin<Box<dyn Stream<Item = Result<Event, Status>> + Send>>;

#[tonic::async_trait]
impl Arisa for ArisaService {
    type SubscribeEventsStream = EventStream;

    async fn health_check(&self, _: Request<()>) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }

    async fn subscribe_events(
        &self,
        _: Request<SubscribeEventsRequest>,
    ) -> Result<Response<Self::SubscribeEventsStream>, Status> {
        let stream =
            BroadcastStream::new(self.events.subscribe()).filter_map(|event| event.ok().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn reply(&self, request: Request<ReplyRequest>) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        self.enqueue(Action::Reply {
            channel_id: request.channel_id,
            message: request.message,
            thread_id: request.thread_id,
        })
    }

    async fn mark_read(&self, request: Request<MarkReadRequest>) -> Result<Response<()>, Status> {
        self.enqueue(Action::MarkRead {
            channel_id: request.into_inner().channel_id,
        })
    }

    async fn enter_channel(
        &self,
        request: Request<EnterChannelRequest>,
    ) -> Result<Response<()>, Status> {
        self.enqueue(Action::EnterChannel {
            channel_id: request.into_inner().channel_id,
        })
    }

    async fn send_media(&self, request: Request<SendMediaRequest>) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        let mode = MediaMode::try_from(request.mode)
            .map_err(|_| Status::invalid_argument("invalid media mode"))?;
        let files = media::store(&self.temp_dir, request.files)
            .await
            .map_err(Status::from)?;
        self.enqueue(Action::SendMedia {
            channel_id: request.channel_id,
            files,
            multiple: mode == MediaMode::Multiple,
        })
    }

    async fn send_media_by_chunk(
        &self,
        request: Request<tonic::Streaming<SendMediaByChunkRequest>>,
    ) -> Result<Response<()>, Status> {
        let media = media::store_chunks(&self.temp_dir, request.into_inner()).await?;
        self.enqueue(Action::SendMedia {
            channel_id: media.channel_id,
            files: media.files,
            multiple: media.multiple,
        })
    }

    async fn get_user(&self, request: Request<GetUserRequest>) -> Result<Response<Member>, Status> {
        let request = request.into_inner();
        let database = self.database.clone();
        let member =
            blocking(move || database.get_user(request.channel_id, request.user_id)).await?;
        Ok(Response::new(member))
    }

    async fn get_users(
        &self,
        request: Request<GetUsersRequest>,
    ) -> Result<Response<GetUsersResponse>, Status> {
        let request = request.into_inner();
        let database = self.database.clone();
        let members =
            blocking(move || database.get_users(request.channel_id, &request.user_ids)).await?;
        Ok(Response::new(GetUsersResponse { members }))
    }

    async fn get_channel(
        &self,
        request: Request<GetChannelRequest>,
    ) -> Result<Response<Channel>, Status> {
        let channel_id = request.into_inner().channel_id;
        let database = self.database.clone();
        let channel = blocking(move || database.get_channel(channel_id)).await?;
        Ok(Response::new(channel))
    }

    async fn get_message(
        &self,
        request: Request<GetMessageRequest>,
    ) -> Result<Response<Event>, Status> {
        let request = request.into_inner();
        let database = self.database.clone();
        let event =
            blocking(move || database.get_message(request.channel_id, request.message_id)).await?;
        Ok(Response::new(event))
    }

    async fn get_messages(
        &self,
        request: Request<GetMessagesRequest>,
    ) -> Result<Response<GetMessagesResponse>, Status> {
        let request = request.into_inner();
        let database = self.database.clone();
        let events =
            blocking(move || database.get_messages(request.channel_id, &request.message_ids))
                .await?;
        Ok(Response::new(GetMessagesResponse { events }))
    }

    async fn get_channel_members(
        &self,
        request: Request<GetChannelMembersRequest>,
    ) -> Result<Response<ChannelMembers>, Status> {
        let channel_id = request.into_inner().channel_id;
        let database = self.database.clone();
        let members = blocking(move || database.get_channel_members(channel_id)).await?;
        Ok(Response::new(members))
    }

    async fn raw_query(
        &self,
        request: Request<RawQueryRequest>,
    ) -> Result<Response<RawQueryResponse>, Status> {
        let request = request.into_inner();
        if request.sql.trim().is_empty() {
            return Err(Status::invalid_argument("sql cannot be empty"));
        }
        let limit = request.limit.unwrap_or(100).clamp(1, 10_000) as usize;
        let database = self.database.clone();
        let rows_json = blocking(move || {
            database
                .raw_query(&request.sql, limit)
                .map(|rows| rows.into_iter().map(|row| row.to_string()).collect())
        })
        .await?;
        Ok(Response::new(RawQueryResponse { rows_json }))
    }

    async fn decrypt(
        &self,
        request: Request<DecryptRequest>,
    ) -> Result<Response<DecryptResponse>, Status> {
        let request = request.into_inner();
        Ok(Response::new(DecryptResponse {
            plaintext: self
                .database
                .decrypt(&request.ciphertext, request.enc, request.user_id),
        }))
    }

    async fn get_credential(&self, _: Request<()>) -> Result<Response<Credential>, Status> {
        let credential = credential::read(&self.app_path)
            .await
            .map_err(Status::internal)?;
        Ok(Response::new(credential))
    }
}

async fn blocking<T>(
    operation: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, Status>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| Status::internal(format!("database task failed: {error}")))?
        .map_err(Status::internal)
}
