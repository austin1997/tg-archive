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
    def __init__(self, input_queue: utils.OrderedPriorityQueue, client: TelegramClient, database: db.AsyncDB, downloader: FastTelethon.ParallelTransferrer, media_tmp_dir: str):
        self.input_queue = input_queue
        self.client = client
        self.db = database
        self.media_tmp_dir = media_tmp_dir
        self.downloader = downloader
    async def run(self):
        try:
            while True:
                _, temp = await self.input_queue.get()
                msg, media_dir = temp
                if msg is None:
                    break
                logging.info("Handling media in chat id: {}, msg id: {}".format(msg.chat_id, msg.id))
                media_id = utils.get_media_id(msg)
                if media_id is None or msg.file is None:
                    logging.info("media in chat: {} msg: {} disappeared.".format(msg.chat_id, msg.id))
                    continue
                cache = await self.db.get_media(media_id)
                if cache is not None:
                    logging.info("found media id: {} in cache".format(media_id))
                    await self.db.remove_pending_message(msg.chat_id, msg.id)
                    continue
                media = await self._handle_message(msg, media_id, media_dir)
                if media is not None:
                    await self.db.insert_media(media)
                    await self.db.remove_pending_message(msg.chat_id, msg.id)
                    await self.db.commit()
        finally:
            await self.downloader._cleanup()
            logging.info("MediaWorker cancelled.")

    async def _handle_message(self, msg: telethon.tl.custom.Message, media_id: int, media_dir: str) -> db.Media:
        try:
            if msg is None:
                return None
            logging.info("checking media id: {}, name: {} in cache".format(media_id, msg.file.name))
            if media_id is None:
                raise
            logging.info("downloading media id: {} from chat id: {} msg id: {}".format(media_id, msg.chat_id, msg.id))
            # self.client.download_media(msg, file=self.media_tmp_dir)
            basename, fname, thumb = await self._download_media(msg, media_dir)
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
                    return await self._handle_message(msg, utils.get_media_id(msg), media_dir)
        except (errors.FilerefUpgradeNeededError, errors.FileReferenceExpiredError) as e:
            msg = await self.client.get_messages(await msg.get_input_chat(), ids=msg.id)
            return await self._handle_message(msg, utils.get_media_id(msg), media_dir)
        except Exception as e:
            logging.error(
                "error downloading media: #{}: {}".format(msg.id, e))
            traceback.print_exc()
            await asyncio.sleep(300)    # Sleep for 5 minutes
            msg = await self.client.get_messages(await msg.get_input_chat(), ids=msg.id)
            await self.downloader._cleanup()
            return await self._handle_message(msg, utils.get_media_id(msg), media_dir)

    async def _download_with_progress(self, msg: telethon.tl.custom.Message, media_dir, rename_prefix=""):
        def progress_callback(diff, total):
            if total is not None:
                pbar.total = total
            pbar.update(diff)

        with logging_redirect_tqdm():
            with tqdm(desc=msg.file.name, total=msg.file.size, unit='B', unit_scale=True, unit_divisor=1024, miniters=1) as pbar:
                basename = msg.file.name
                if basename is None:
                    basename = str(utils.get_media_id(msg)) + telethon.utils.get_extension(msg.media)
                tmpfile_path = await self.downloader.download(msg, download_folder=self.media_tmp_dir, filename=f"{utils.get_media_id(msg)}", progress_callback=progress_callback)
                destination_path = os.path.join(media_dir, f"{rename_prefix}{basename}")
                base, extension = os.path.splitext(destination_path)
                i = 1
                while os.path.exists(destination_path): # Create a new name if the file already exists
                    logging.info(f"file {destination_path} already exists")
                    destination_path = f"{base}_{i}{extension}"
                    i += 1
                # Move the file
                logging.info(f"moving {tmpfile_path} to {destination_path}")
                shutil.move(tmpfile_path, destination_path)
                return basename, os.path.basename(destination_path)

    async def _download_media(self, msg: telethon.tl.custom.Message, media_dir: str):
        """
        Download a media / file attached to a message and return its original
        filename, sanitized name on disk, and the thumbnail (if any). 
        """
        # Download the media to the temp dir and copy it back as
        # there does not seem to be a way to get the canonical
        # filename before the download.
        basename, newname = await self._download_with_progress(msg, media_dir)

        # If it's a photo, download the thumbnail.
        tname = None
        # if isinstance(msg.media, telethon.tl.types.MessageMediaPhoto):
        #     _, tname = await self._download_with_progress(msg, "thumb_", thumb=1)

        return basename, newname, tname