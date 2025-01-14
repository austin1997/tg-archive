import asyncio
import logging
import json
import os
import shutil
from tqdm.asyncio import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
import telethon
import telethon.tl.custom
import telethon.tl.types
from telethon import TelegramClient, errors
from tgarchive import db, utils, FastTelethon
import traceback

class MediaWorker:
    def __init__(self, input_queue: asyncio.Queue, client: TelegramClient, auth_key_cache: dict, media_downloading: set, database: db.DB, media_dir: str, media_tmp_dir: str):
        self.input_queue = input_queue
        self.client = client
        self.db = database
        self.media_dir = media_dir
        self.media_tmp_dir = media_tmp_dir
        self.downloader = FastTelethon.ParallelTransferrer(self.client, auth_key_cache)
        self.media_downloading = media_downloading
    
    async def run(self):
        try:
            while True:
                msg: telethon.tl.custom.Message = await self.input_queue.get()
                if msg is None:
                    break
                media_id = utils.get_media_id(msg)
                if media_id is None or msg.file is None:
                    logging.info("media in chat: {} msg: {} disappeared.".format(msg.chat_id, msg.id))
                    continue
                if media_id in self.media_downloading:
                    logging.info("media id: {} is already downloading".format(media_id))
                    continue
                cache = self.db.get_media(media_id)
                if cache is not None:
                    logging.info("found media id: {} in cache".format(media_id))
                    self.db.remove_pending_message(msg.chat_id, msg.id)
                    continue
                self.media_downloading.add(media_id)
                media = await self._handle_message(msg, media_id)
                self.db.insert_media(media)
                self.db.remove_pending_message(msg.chat_id, msg.id)
                self.db.commit()
                self.media_downloading.remove(media_id)
        finally:
            await self.downloader._cleanup()
            logging.info("MediaWorker cancelled.")

    async def _handle_message(self, msg: telethon.tl.custom.Message, media_id: int) -> db.Media:
        try:
            logging.info("checking media id: {}, name: {} in cache".format(media_id, msg.file.name))
            if media_id is None:
                raise
            logging.info("downloading media id: {} from msg id: {}".format(media_id, msg.id))
            basename, fname, thumb = await self._download_media(msg)
            return db.Media(
                id=media_id,
                type=msg.file.mime_type if hasattr(msg, "file") and hasattr(msg.file, "mime_type") else "photo",
                url=fname,
                title=basename,
                description=None,
                thumb=thumb
            )
        except (errors.FloodWaitError, errors.FloodPremiumWaitError) as e:
                    logging.info(f"Sleeping for {e.seconds + 60} seconds." + e._fmt_request(e.request))
                    await asyncio.sleep(e.seconds + 60)
                    # retry download
                    return await self._handle_message(msg, utils.get_media_id(msg))
        except (errors.FilerefUpgradeNeededError, errors.FileReferenceExpiredError) as e:
            msg = self.client.get_messages(await msg.get_input_chat(), ids=msg.id)
            return await self._handle_message(msg, utils.get_media_id(msg))
        except Exception as e:
            logging.error(
                "error downloading media: #{}: {}".format(msg.id, e))
            traceback.print_exc()

    async def _download_with_progress(self, msg: telethon.tl.custom.Message, rename_prefix="", **kwargs):
        def progress_callback(diff, total):
            if total is not None:
                pbar.total = total
            pbar.update(diff)

        with logging_redirect_tqdm():
            with tqdm(desc=msg.file.name, total=msg.file.size, unit='B', unit_scale=True, unit_divisor=1024, miniters=1) as pbar:
                basename = msg.file.name
                if basename is None:
                    basename = str(utils.get_media_id(msg)) + telethon.utils.get_extension(msg.media)
                tmpfile_path = await self.downloader.download(msg, download_folder=self.media_tmp_dir, filename=f"{utils.get_media_id(msg)}", progress_callback=progress_callback, **kwargs)
                destination_path = os.path.join(self.media_dir, f"{rename_prefix}{basename}")
                if os.path.exists(destination_path): # Create a new name if the file already exists
                    base, extension = os.path.splitext(destination_path)
                    i = 1
                    new_path = f"{base}_{i}{extension}"
                    while os.path.exists(new_path):
                        i += 1
                        new_path = f"{base}_{i}{extension}"
                        destination_path = new_path
                # Move the file
                logging.info(f"moving {tmpfile_path} to {destination_path}")
                shutil.move(tmpfile_path, destination_path)
                return basename, os.path.basename(destination_path)

    async def _download_media(self, msg: telethon.tl.custom.Message):
        """
        Download a media / file attached to a message and return its original
        filename, sanitized name on disk, and the thumbnail (if any). 
        """
        # Download the media to the temp dir and copy it back as
        # there does not seem to be a way to get the canonical
        # filename before the download.
        basename, newname = await self._download_with_progress(msg)

        # If it's a photo, download the thumbnail.
        tname = None
        if isinstance(msg.media, telethon.tl.types.MessageMediaPhoto):
            _, tname = await self._download_with_progress(msg, "thumb_", thumb=1)

        return basename, newname, tname