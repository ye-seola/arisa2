from __future__ import annotations

import asyncio
import inspect
import json
import traceback
import types
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, Literal, TypeVar

import betterproto2
from grpclib.client import Channel
from grpclib.const import Status
from grpclib.exceptions import GRPCError, StreamTerminatedError

from airi.context import AiriContext, Event, Filter, Handler, Middleware, Next
from airi.errors import AiriError
from airi.generated.arisa import v1 as proto
from airi.media import MediaFilesInput, normalize, read

T = TypeVar("T")
E = TypeVar("E")

MEDIA_CHUNK_SIZE = 1024 * 1024


@dataclass(slots=True)
class _Route:
    event_type: type[Any]
    handler: Handler[Any]
    filters: tuple[Filter[Any], ...]


class BotClient:
    def __init__(self, target: str, *, reconnect: bool = True) -> None:
        self.target = target
        self.reconnect = reconnect
        self._host, self._port = _split_target(target)
        self._channel: Channel | None = None
        self._stub: proto.ArisaStub | None = None
        self._routes: list[_Route] = []
        self._middlewares: list[Middleware] = []
        self._closed = False

    async def __aenter__(self) -> BotClient:  # noqa: PYI034
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> None:
        await self.close()

    async def connect(self) -> None:
        if self._channel is not None:
            return
        self._closed = False
        channel = Channel(self._host, self._port)
        stub = proto.ArisaStub(channel)
        try:
            await stub.health_check()
        except BaseException:
            channel.close()
            raise
        self._channel = channel
        self._stub = stub
        print(f"connected to {self.target}")

    async def close(self) -> None:
        self._closed = True
        self._disconnect()

    def _disconnect(self) -> None:
        if self._channel is not None:
            self._channel.close()
        self._channel = None
        self._stub = None

    def on(
        self,
        event_type: type[E],
        *filters: Filter[E],
    ) -> Callable[[Handler[E]], Handler[E]]:
        def decorator(handler: Handler[E]) -> Handler[E]:
            self._routes.append(_Route(event_type, handler, filters))
            return handler

        return decorator

    def middleware(self, middleware: Middleware) -> Middleware:
        self._middlewares.append(middleware)
        return middleware

    async def dispatch(self, event: Event) -> None:
        context = AiriContext(bot=self, event=event, envelope=event)

        async def call_routes(current: AiriContext[Event]) -> None:
            for route in self._routes:
                matched = _match_event(current.envelope, route.event_type)
                if matched is None:
                    continue
                route_context = current._with_event(matched)
                if await _passes(route.filters, route_context):
                    try:
                        await route.handler(route_context)
                    except Exception:  # noqa: BLE001
                        traceback.print_exc()

        next_handler: Next = call_routes
        for middleware in reversed(self._middlewares):
            inner = next_handler

            async def wrapped(
                current: AiriContext[Event],
                middleware: Middleware = middleware,
                inner: Next = inner,
            ) -> None:
                await middleware(current, inner)

            next_handler = wrapped

        await next_handler(context)

    async def run(self) -> None:
        self._closed = False
        while not self._closed:
            try:
                await self._run_once()
            except (GRPCError, StreamTerminatedError, OSError) as error:
                if self._closed:
                    return
                if not self.reconnect or not _is_reconnectable(error):
                    if isinstance(error, GRPCError):
                        raise AiriError.from_rpc(error) from error
                    raise
            else:
                if not self.reconnect or self._closed:
                    return

            self._disconnect()
            await asyncio.sleep(1)

    async def _run_once(self) -> None:
        stub = await self._get_stub()
        stream = stub.subscribe_events()
        async for event in stream:
            await self.dispatch(_unwrap_event(event))

    async def health_check(self) -> None:
        await self._call((await self._get_stub()).health_check())

    async def reply(
        self,
        channel_id: int,
        message: str,
        *,
        thread_id: int | None = None,
    ) -> None:
        request = proto.ReplyRequest(
            channel_id=channel_id,
            message=message,
            thread_id=thread_id,
        )
        await self._call((await self._get_stub()).reply(request))

    async def read(self, channel_id: int) -> None:
        await self._call(
            (await self._get_stub()).mark_read(
                proto.MarkReadRequest(channel_id=channel_id)
            )
        )

    async def enter_channel(self, channel_id: int) -> None:
        await self._call(
            (await self._get_stub()).enter_channel(
                proto.EnterChannelRequest(channel_id=channel_id)
            )
        )

    async def reply_media(
        self,
        channel_id: int,
        file: MediaFilesInput,
        *,
        name: str | None = None,
        mode: Literal["single", "multiple"] | None = None,
        mime: str | None = None,
    ) -> None:
        if mode not in {None, "single", "multiple"}:
            raise ValueError("mode must be 'single' or 'multiple'")

        items = normalize(file, name=name, mime=mime)
        selected_mode = mode or ("multiple" if len(items) > 1 else "single")
        media_mode = (
            proto.MediaMode.MULTIPLE
            if selected_mode == "multiple"
            else proto.MediaMode.SINGLE
        )

        async def requests():
            yield proto.SendMediaByChunkRequest(
                metadata=proto.SendMediaByChunkMetadata(
                    channel_id=channel_id,
                    file_count=len(items),
                    mode=media_mode,
                )
            )
            for item in items:
                data = await asyncio.to_thread(read, item)
                yield proto.SendMediaByChunkRequest(
                    file_metadata=proto.MediaFileMetadata(
                        file_name=item.name or "",
                        file_size=len(data),
                        content_type=item.mime or "",
                    )
                )
                for offset in range(0, len(data), MEDIA_CHUNK_SIZE):
                    yield proto.SendMediaByChunkRequest(
                        chunk=data[offset : offset + MEDIA_CHUNK_SIZE]
                    )

        await self._call((await self._get_stub()).send_media_by_chunk(requests()))

    async def get_user(self, channel_id: int, user_id: int) -> proto.Member:
        return await self._call(
            (await self._get_stub()).get_user(
                proto.GetUserRequest(channel_id=channel_id, user_id=user_id)
            )
        )

    async def get_users(
        self,
        channel_id: int,
        user_ids: Sequence[int],
    ) -> list[proto.Member]:
        response = await self._call(
            (await self._get_stub()).get_users(
                proto.GetUsersRequest(channel_id=channel_id, user_ids=list(user_ids))
            )
        )
        return list(response.members)

    async def get_channel(self, channel_id: int) -> proto.Channel:
        return await self._call(
            (await self._get_stub()).get_channel(
                proto.GetChannelRequest(channel_id=channel_id)
            )
        )

    async def get_message(self, channel_id: int, message_id: int) -> Event:
        response = await self._call(
            (await self._get_stub()).get_message(
                proto.GetMessageRequest(
                    channel_id=channel_id,
                    message_id=message_id,
                )
            )
        )
        return _unwrap_event(response)

    async def get_messages(
        self,
        channel_id: int,
        message_ids: Sequence[int],
    ) -> list[Event]:
        response = await self._call(
            (await self._get_stub()).get_messages(
                proto.GetMessagesRequest(
                    channel_id=channel_id,
                    message_ids=list(message_ids),
                )
            )
        )
        return [_unwrap_event(event) for event in response.events]

    async def get_channel_member_ids(self, channel_id: int) -> list[int]:
        response = await self._call(
            (await self._get_stub()).get_channel_members(
                proto.GetChannelMembersRequest(channel_id=channel_id)
            )
        )
        return list(response.active_member_ids)

    async def raw_query(
        self,
        sql: str,
        *,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        request = proto.RawQueryRequest(sql=sql, limit=limit)
        response = await self._call((await self._get_stub()).raw_query(request))
        return [json.loads(row) for row in response.rows_json]

    async def decrypt(
        self,
        ciphertext: str,
        enc: int,
        *,
        user_id: int | None = None,
    ) -> str:
        request = proto.DecryptRequest(
            ciphertext=ciphertext,
            enc=enc,
            user_id=user_id,
        )
        response = await self._call((await self._get_stub()).decrypt(request))
        return response.plaintext

    async def get_credential(self) -> proto.Credential:
        return await self._call((await self._get_stub()).get_credential())

    async def _get_stub(self) -> proto.ArisaStub:
        await self.connect()
        assert self._stub is not None
        return self._stub

    async def _call(self, call: Awaitable[T]) -> T:
        try:
            return await call
        except GRPCError as error:
            raise AiriError.from_rpc(error) from error


def _unwrap_event(event: proto.Event) -> Event:
    if event.message is not None:
        return event.message
    if event.feed is not None:
        return event.feed
    raise ValueError("event has no value")


def _match_event(event: Event, event_type: type[Any]) -> Any | None:
    if isinstance(event, event_type):
        return event
    if not isinstance(event, proto.FeedEvent):
        return None
    if event.feed is None:
        return None
    _, value = betterproto2.which_one_of(event.feed, "value")
    return value if isinstance(value, event_type) else None


def _is_reconnectable(
    error: GRPCError | StreamTerminatedError | OSError,
) -> bool:
    return not isinstance(error, GRPCError) or error.status == Status.UNAVAILABLE


def _split_target(target: str) -> tuple[str, int]:
    host, separator, port = target.rpartition(":")
    if not separator or not host or not port.isdecimal():
        raise ValueError("target must use the form 'host:port'")
    return host.removeprefix("[").removesuffix("]"), int(port)


async def _passes(
    filters: tuple[Filter[Any], ...],
    context: AiriContext[Any],
) -> bool:
    for filter_ in filters:
        result = filter_(context)
        if inspect.isawaitable(result):
            result = await result
        if not result:
            return False
    return True
