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
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

from telethon import TelegramClient, helpers, errors
from telethon import utils as telethon_utils
from telethon.crypto import AuthKey
from telethon.network import MTProtoSender
import telethon.tl.custom
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
    request: GetFileRequest

    def __init__(
        self,
        client: TelegramClient,
        sender: MTProtoSender,
        auth_key: Optional[AuthKey] = None,
    ) -> None:
        self.sender = sender
        self.client = client
        self.auth_key = auth_key

    async def run(self, queue: asyncio.Queue, file: BinaryIO, connection_created: datetime, file_hashes: Dict[int, FileHash] = None, process_callback = None):
        while not queue.empty() and (datetime.now() - connection_created).total_seconds() < MAX_CONNECTION_LIFETIME:
            request: GetFileRequest = await queue.get()
            offset = request.offset
            # logging.info(f"request limit = {self.request.limit}")
            if self.client.session.takeout_id is not None:
                request = InvokeWithTakeoutRequest(self.client.session.takeout_id, request)
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

    async def disconnect(self) -> Awaitable[None]:
        return await self.sender.disconnect()

class ParallelTransferrer:
    client: TelegramClient
    loop: asyncio.AbstractEventLoop
    senders: Dict[int, Tuple[List[DownloadSender], datetime]]
    sender_created: datetime

    def __init__(self, client: TelegramClient) -> None:
        self.client = client
        self.loop = self.client.loop
        self.senders = {}
        self.connection_lock = asyncio.Lock()

    async def _cleanup(self, dc_id: Optional[int] = None) -> None:
        # await asyncio.gather(*[sender.disconnect() for sender in self.senders])
        if dc_id is not None:
            if dc_id in self.senders:
                for sender in self.senders[dc_id][0]:
                    await sender.disconnect()
                self.senders.pop(dc_id)
        else:
            for item in self.senders.values():
                for sender in item[0]:
                    await sender.disconnect()
            self.senders.clear()

    @staticmethod
    def _get_connection_count(
        file_size: int, max_count: int = 20, full_size: int = 100 * 1024 * 1024
    ) -> int:
        if file_size > full_size:
            return max_count
        return math.ceil((file_size / full_size) * max_count)

    async def _init_download(
        self, connections: int, dc_id: int
    ) -> datetime:
        async with self.connection_lock:
            curr_time = datetime.now()
            if self.senders.get(dc_id, None) is not None:
                time_diff = curr_time - self.senders[dc_id][1]
                if time_diff.total_seconds() < MAX_CONNECTION_LIFETIME:
                    return self.senders[dc_id][1]
                logging.info("Clearing long-lasting connections and reconnect")

            await self._cleanup(dc_id)
            logging.info(f"Creating new connections to DC {dc_id}")
            # The first cross-DC sender will export+import the authorization, so we always create it
            # before creating any other senders.
            sender = await self._create_download_sender(dc_id)
            senders = [
                sender,
                *await asyncio.gather(
                    *[
                        self._create_download_sender(
                            dc_id, sender.auth_key
                        )
                        for i in range(1, connections)
                    ]
                ),
            ]
            self.senders[dc_id] = (senders, curr_time)
            return curr_time

    async def _create_download_sender(
        self,
        dc_id: int,
        auth_key: Optional[AuthKey] = None,
    ) -> DownloadSender:
        sender = await self._create_sender(dc_id, auth_key=auth_key)
        return DownloadSender(
            self.client,
            sender[0],
            auth_key=sender[1]
        )

    async def _create_sender(self, dc_id: int, auth_key: AuthKey) -> Tuple[MTProtoSender, AuthKey]:
        dc = await self.client._get_dc(dc_id)
        if auth_key is None and dc_id == self.client.session.dc_id:
            auth_key = self.client.session.auth_key
        sender = MTProtoSender(auth_key, loggers=self.client._log)
        await sender.connect(
            self.client._connection(
                dc.ip_address,
                dc.port,
                dc.id,
                loggers=self.client._log,
                proxy=self.client._proxy,
            )
        )
        if auth_key is None:
            me = await self.client.get_me()
            logging.info(f"Migrating to DC {dc_id}, client DC: {self.client.session.dc_id}")
            auth = await self.client(ExportAuthorizationRequest(dc_id))
            logging.info("Exported authorization")
            self.client._init_request.query = ImportAuthorizationRequest(
                id=auth.id, bytes=auth.bytes
            )
            req = InvokeWithLayerRequest(LAYER, self.client._init_request)
            # if self.client.session.takeout_id is not None:
            #     req = InvokeWithTakeoutRequest(self.client.session.takeout_id, req)
            logging.info("Importing authorization")
            await sender.send(req)
            auth_key = sender.auth_key
        return (sender, auth_key)

    async def download(
        self,
        msg: telethon.tl.custom.Message,
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
        part_count = (file_size + part_size - 1) // part_size
        connection_count = 8

        out_file.seek(0)
        out_file.write(b"\0" * file_size)
        out_file.seek(0)
        out_file.truncate(file_size)

        # file_hashes: list[FileHash] = await self.client(GetFileHashesRequest(file, 0))
        # file_hashes_dict: dict[int, FileHash] = {}
        # for file_hash in file_hashes:
        #     file_hashes_dict[file_hash.offset] = file_hash

        tasks = []
        try:
            queue = asyncio.Queue()
            for i in range(0, file_size, part_size):
                queue.put_nowait(GetFileRequest(file, i, limit=part_size))

            while not queue.empty():
                connection_created_time = await self._init_download(connection_count, dc_id)
                tasks = []
                for sender in self.senders[dc_id][0]:
                    task = asyncio.create_task(sender.run(queue, out_file, connection_created_time, None, process_callback))
                    tasks.append(task)
                await asyncio.gather(*tasks)
            await queue.join()
        finally:
            # Cancel our worker tasks.
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks)
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
