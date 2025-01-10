"""
> Based on parallel_file_transfer.py from mautrix-telegram, with permission to distribute under the MIT license
> Copyright (C) 2019 Tulir Asokan - https://github.com/tulir/mautrix-telegram
"""
import asyncio
import hashlib
import inspect
import logging
import math
import os
import hashlib
from datetime import datetime
from collections import defaultdict
from typing import (
    AsyncGenerator,
    Awaitable,
    BinaryIO,
    DefaultDict,
    List,
    Optional,
    Tuple,
    Union,
)

from telethon import TelegramClient, helpers, errors
from telethon import utils as telethon_utils
from telethon.crypto import AuthKey
from telethon.network import MTProtoSender
from telethon.tl import types
from telethon.tl.alltlobjects import LAYER
from telethon.tl.functions import InvokeWithLayerRequest, InvokeWithTakeoutRequest
from telethon.tl.functions.auth import (
    ExportAuthorizationRequest,
    ImportAuthorizationRequest,
)
from telethon.tl.functions.upload import (
    GetFileRequest,
    SaveBigFilePartRequest,
    SaveFilePartRequest,
    GetFileHashesRequest
)
from telethon.tl.types import (
    Document,
    InputDocumentFileLocation,
    InputFile,
    InputFileBig,
    InputFileLocation,
    InputPeerPhotoFileLocation,
    InputPhotoFileLocation,
    TypeInputFile,
    FileHash
)

from . import utils

log: logging.Logger = logging.getLogger("FastTelethon")

TypeLocation = Union[
    Document,
    InputDocumentFileLocation,
    InputPeerPhotoFileLocation,
    InputFileLocation,
    InputPhotoFileLocation,
]

MAX_CONNECTION_LIFETIME: int = 3600


class DownloadSender:
    client: TelegramClient
    sender: MTProtoSender
    request: Union[GetFileRequest, InvokeWithTakeoutRequest]

    def __init__(
        self,
        client: TelegramClient,
        sender: MTProtoSender,
        file: TypeLocation,
        offset: int,
        limit: int,
    ) -> None:
        self.sender = sender
        self.client = client
        self.request = GetFileRequest(file, offset=offset, limit=limit)
    
    def reset_file(self, file: TypeLocation, part_size):
        self.request = GetFileRequest(file, 0, limit=part_size)

    async def run(self, queue: asyncio.Queue, file: BinaryIO, connection_created: datetime, file_hashes: dict[int, FileHash] = None, process_callback = None):
        while not queue.empty() and (datetime.now() - connection_created).total_seconds() < MAX_CONNECTION_LIFETIME:
            offset = await queue.get()
            self.request.offset = offset
            # logging.info(f"request limit = {self.request.limit}")
            if self.client.session.takeout_id is None:
                request = self.request
            else:
                request = InvokeWithTakeoutRequest(self.client.session.takeout_id, self.request)
            verified = False
            while not verified:
                result = None
                try:
                    # logging.info("invoking request")
                    result = await self.client._call(self.sender, request)
                except (errors.FloodWaitError, errors.FloodPremiumWaitError) as e:
                    logging.info(f"Sleeping for {e.seconds + 60} seconds." + e._fmt_request(e.request))
                    await asyncio.sleep(e.seconds + 60)
                    # retry download
                    result = await self.client._call(self.sender, request)
                assert result is not None
                result = result.bytes
                if not file_hashes:
                    break
                verified = True
                hash_offset = offset
                while hash_offset < offset + len(result):
                    file_hash = file_hashes.get(hash_offset, None)
                    if not file_hash:
                        logging.info(f"file_hash does not found at {hash_offset}. Skipped")
                        break
                    if file_hash.offset + file_hash.limit > offset + len(result):
                        logging.info(f"file_hash exceed downloaded part, offset: {file_hash.offset}, limit: {file_hash.limit}, local_offset: {offset}. Skipped")
                        break
                    local_hash = hashlib.sha256(result[hash_offset - offset:hash_offset + file_hash.limit]).digest()
                    if local_hash != file_hash.hash:
                        logging.info(f"file_hash verified failed, offset: {file_hash.offset}, limit: {file_hash.limit}, local_offset: {offset}. Redownloading...")
                        verified = False
                        break
                    hash_offset += file_hash.limit

            file.seek(offset)
            # TODO: async write
            file.write(result)
            if process_callback:
                r = process_callback(len(result), None)
                if inspect.isawaitable(r):
                    try:
                        await r
                    except BaseException:
                        pass
            queue.task_done()

    def disconnect(self) -> Awaitable[None]:
        return self.sender.disconnect()


class UploadSender:
    client: TelegramClient
    sender: MTProtoSender
    request: Union[SaveFilePartRequest, SaveBigFilePartRequest]
    part_count: int
    stride: int
    previous: Optional[asyncio.Task]
    loop: asyncio.AbstractEventLoop

    def __init__(
        self,
        client: TelegramClient,
        sender: MTProtoSender,
        file_id: int,
        part_count: int,
        big: bool,
        index: int,
        stride: int,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self.client = client
        self.sender = sender
        self.part_count = part_count
        if big:
            self.request = SaveBigFilePartRequest(file_id, index, part_count, b"")
        else:
            self.request = SaveFilePartRequest(file_id, index, b"")
        self.stride = stride
        self.previous = None
        self.loop = loop

    async def next(self, data: bytes) -> None:
        if self.previous:
            await self.previous
        self.previous = self.loop.create_task(self._next(data))

    async def _next(self, data: bytes) -> None:
        self.request.bytes = data
        await self.client._call(self.sender, self.request)
        self.request.file_part += self.stride

    async def disconnect(self) -> None:
        if self.previous:
            await self.previous
        return await self.sender.disconnect()


class ParallelTransferrer:
    client: TelegramClient
    loop: asyncio.AbstractEventLoop
    dc_id: int
    senders: Optional[List[Union[DownloadSender, UploadSender]]]
    auth_key: AuthKey
    upload_ticker: int
    sender_pool: dict[int, Tuple[List[Union[DownloadSender, UploadSender]], datetime]]

    def __init__(self, client: TelegramClient) -> None:
        self.client = client
        self.loop = self.client.loop
        self.auth_key = None
        self.senders = None
        self.upload_ticker = 0
        self.sender_pool = {}

    async def _cleanup(self) -> None:
        await asyncio.gather(*[sender.disconnect() for sender in self.senders])
        self.senders = None
        for dc, senders in self.sender_pool.items():
            senders, _ = senders
            for sender in senders:
                await sender.disconnect()
        self.sender_pool.clear()

    @staticmethod
    def _get_connection_count(
        file_size: int, max_count: int = 20, full_size: int = 100 * 1024 * 1024
    ) -> int:
        if file_size > full_size:
            return max_count
        return math.ceil((file_size / full_size) * max_count)

    async def _init_download(
        self, connections: int, dc_id: int, file: TypeLocation, part_size: int
    ) -> datetime:
        curr_time = datetime.now()
        self.senders, created_time = self.sender_pool.get(dc_id, (None, None))
        if self.senders is not None:
            time_diff = curr_time - created_time
            if time_diff.total_seconds() < 1800:
                for sender in self.senders:
                    sender.reset_file(file, part_size)
                return created_time
            else:
                logging.info("Clearing long-lasting connections and reconnect")
                for sender in self.senders:
                    await sender.disconnect()
        
        self.auth_key = None
        if self.client.session.dc_id == dc_id:
            self.auth_key = self.client.session.auth_key
        # The first cross-DC sender will export+import the authorization, so we always create it
        # before creating any other senders.
        self.senders = [
            await self._create_download_sender(
                dc_id, file, part_size
            ),
            *await asyncio.gather(
                *[
                    self._create_download_sender(
                        dc_id, file, part_size
                    )
                    for i in range(1, connections)
                ]
            ),
        ]
        self.sender_pool[dc_id] = (self.senders, curr_time)
        return curr_time

    async def _create_download_sender(
        self,
        dc_id: int,
        file: TypeLocation,
        part_size: int,
    ) -> DownloadSender:
        return DownloadSender(
            self.client,
            await self._create_sender(dc_id),
            file,
            0,
            part_size
        )

    async def _init_upload(
        self, connections: int, file_id: int, part_count: int, big: bool
    ) -> None:
        self.auth_key = self.client.session.auth_key
        self.senders = [
            await self._create_upload_sender(file_id, part_count, big, 0, connections),
            *await asyncio.gather(
                *[
                    self._create_upload_sender(file_id, part_count, big, i, connections)
                    for i in range(1, connections)
                ]
            ),
        ]

    async def _create_upload_sender(
        self, file_id: int, part_count: int, big: bool, index: int, stride: int
    ) -> UploadSender:
        return UploadSender(
            self.client,
            await self._create_sender(self.client.session.dc_id),
            file_id,
            part_count,
            big,
            index,
            stride,
            loop=self.loop,
        )

    async def _create_sender(self, dc_id: int) -> MTProtoSender:
        dc = await self.client._get_dc(dc_id)
        sender = MTProtoSender(self.auth_key, loggers=self.client._log)
        await sender.connect(
            self.client._connection(
                dc.ip_address,
                dc.port,
                dc.id,
                loggers=self.client._log,
                proxy=self.client._proxy,
            )
        )
        if not self.auth_key:
            auth = await self.client(ExportAuthorizationRequest(dc_id))
            self.client._init_request.query = ImportAuthorizationRequest(
                id=auth.id, bytes=auth.bytes
            )
            req = InvokeWithLayerRequest(LAYER, self.client._init_request)
            # if self.client.session.takeout_id is not None:
            #     req = InvokeWithTakeoutRequest(self.client.session.takeout_id, req)
            await sender.send(req)
            self.auth_key = sender.auth_key
        return sender

    async def init_upload(
        self,
        file_id: int,
        file_size: int,
        part_size_kb: Optional[float] = None,
        connection_count: Optional[int] = None,
    ) -> Tuple[int, int, bool]:
        connection_count = connection_count or self._get_connection_count(file_size)
        part_size = (part_size_kb or telethon_utils.get_appropriated_part_size(file_size)) * 1024
        part_count = (file_size + part_size - 1) // part_size
        is_large = file_size > 10 * 1024 * 1024
        await self._init_upload(connection_count, file_id, part_count, is_large)
        return part_size, part_count, is_large

    async def upload(self, part: bytes) -> None:
        await self.senders[self.upload_ticker].next(part)
        self.upload_ticker = (self.upload_ticker + 1) % len(self.senders)

    async def finish_upload(self) -> None:
        await self._cleanup()

    async def download(
        self,
        msg,
        download_folder: str,
        filename: str = None,
        thumb: int = None,
        progress_callback = None
    ):
        if isinstance(msg.media, (types.MessageMediaPhoto, types.Photo)):
            dc_id, location, file_size = utils.get_photo_location(msg.media, thumb)
        elif isinstance(msg.media, (types.MessageMediaDocument, types.Document)):
            dc_id, location, file_size = utils.get_document_location(msg.media)
        else:
            return None

        if filename is None:
            filename = str(utils.get_media_id(msg)) + telethon_utils.get_extension(msg.media)

        if os.path.exists(download_folder):
            if os.path.isfile(download_folder):
                filename = download_folder
            else:
                filename = os.path.join(download_folder, filename)
        else:
            return None

        with open(filename, "wb") as f:
            # We lock the transfers because telegram has connection count limits
            # downloader = ParallelTransferrer(client, dc_id)

            if progress_callback:
                r = progress_callback(0, file_size)
                if inspect.isawaitable(r):
                    try:
                        await r
                    except BaseException:
                        pass
            await self.download_parallel(f, dc_id, location, file_size, part_size_kb=1024, process_callback=progress_callback)
        return filename

    async def download_parallel(
        self,
        out_file: BinaryIO,
        dc_id: int,
        file: TypeLocation,
        file_size: int,
        part_size_kb: Optional[float] = None,
        connection_count: Optional[int] = None,
        process_callback = None
    ):
        part_size = (part_size_kb or telethon_utils.get_appropriated_part_size(file_size)) * 1024
        # connection_count = connection_count or (min(part_count, 8))
        connection_count = 8

        out_file.seek(0)
        out_file.write(b"\0" * file_size)
        out_file.seek(0)
        out_file.truncate(file_size)

        # file_hashes: list[FileHash] = await self.client(GetFileHashesRequest(file, 0))
        # file_hashes_dict: dict[int, FileHash] = {}
        # for file_hash in file_hashes:
        #     file_hashes_dict[file_hash.offset] = file_hash

        try:
            queue = asyncio.Queue()
            for i in range(0, file_size, part_size):
                queue.put_nowait(i)

            while not queue.empty():
                connection_created_time = await self._init_download(connection_count, dc_id, file, part_size)
                tasks = []
                for sender in self.senders:
                    task = asyncio.create_task(sender.run(queue, out_file, connection_created_time, None, process_callback))
                    tasks.append(task)
                await asyncio.gather(*tasks, return_exceptions=True)
            await queue.join()
        finally:
            # Cancel our worker tasks.
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            # await self._cleanup()


parallel_transfer_locks: DefaultDict[int, asyncio.Lock] = defaultdict(
    lambda: asyncio.Lock()
)


def stream_file(file_to_stream: BinaryIO, chunk_size=1024):
    while True:
        data_read = file_to_stream.read(chunk_size)
        if not data_read:
            break
        yield data_read


async def _internal_transfer_to_telegram(
    client: TelegramClient, response: BinaryIO, progress_callback: callable
) -> Tuple[TypeInputFile, int]:
    file_id = helpers.generate_random_long()
    file_size = os.path.getsize(response.name)

    hash_md5 = hashlib.md5()
    uploader = ParallelTransferrer(client)
    part_size, part_count, is_large = await uploader.init_upload(file_id, file_size)
    buffer = bytearray()
    for data in stream_file(response):
        if progress_callback:
            r = progress_callback(response.tell(), file_size)
            if inspect.isawaitable(r):
                try:
                    await r
                except BaseException:
                    pass
        if not is_large:
            hash_md5.update(data)
        if len(buffer) == 0 and len(data) == part_size:
            await uploader.upload(data)
            continue
        new_len = len(buffer) + len(data)
        if new_len >= part_size:
            cutoff = part_size - len(buffer)
            buffer.extend(data[:cutoff])
            await uploader.upload(bytes(buffer))
            buffer.clear()
            buffer.extend(data[cutoff:])
        else:
            buffer.extend(data)
    if len(buffer) > 0:
        await uploader.upload(bytes(buffer))
    await uploader.finish_upload()
    if is_large:
        return InputFileBig(file_id, part_count, filename), file_size
    else:
        return InputFile(file_id, part_count, filename, hash_md5.hexdigest()), file_size

async def upload_file(
    client: TelegramClient,
    file: BinaryIO,
    name,
    progress_callback: callable = None,
) -> TypeInputFile:
    global filename
    filename = name
    return (await _internal_transfer_to_telegram(client, file, progress_callback))[0]
