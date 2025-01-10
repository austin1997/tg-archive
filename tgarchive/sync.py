from io import BytesIO
from sys import exit
import json
import logging
import os
import tempfile
import shutil
import time
import traceback
import asyncio
from typing import Tuple

from PIL import Image
from telethon import TelegramClient, errors
import telethon.tl.custom
import telethon.tl.types

from tqdm.asyncio import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from tgarchive.db import User, Message, Media, Poll, WebPage, DB
from tgarchive import utils,FastTelethon
import tgarchive.worker as worker


class Sync:
    """
    Sync iterates and receives messages from the Telegram group to the
    local SQLite DB.
    """
    config = {}
    db = None

    def __init__(self, config, session_file, db: DB):
        self.config = config
        self.db = db

        self.client = self.new_client(session_file, config)

        media_dir = os.path.abspath(self.config["media_dir"])
        base_dir = os.path.basename(media_dir)
        parent_dir = os.path.dirname(media_dir)
        media_tmp_dir = os.path.join(parent_dir, base_dir + "_tmp")
        if not os.path.exists(media_dir):
            os.mkdir(media_dir)

        if os.path.exists(media_tmp_dir):
            shutil.rmtree(media_tmp_dir)
        os.mkdir(media_tmp_dir)
        self.media_dir = media_dir
        self.media_tmp_dir = media_tmp_dir
        self.downloader = None

    def sync(self, ids=None, from_id=None):
        client = self.client
        try:
            with client:
                if self.config.get("use_takeout", False):
                    max_retry_times = 3
                    retry = 0
                    while retry < max_retry_times:
                        try:
                            with client.takeout(finalize=True, 
                                                contacts=True, 
                                                users=True, 
                                                chats=True, 
                                                megagroups=True, 
                                                channels=True, 
                                                files=True, 
                                                max_file_size=4*1024*1024*1024) as takeout_client:
                                # check if the takeout session gets invalidated
                                self.client = takeout_client
                                self.client.loop.run_until_complete(self._async(ids, from_id))
                                retry = max_retry_times
                        except errors.TakeoutInitDelayError as e:
                            retry += 1
                            logging.info(
                                "please allow the data export request received from Telegram on your device. "
                                "you can also wait for {} seconds.".format(e.seconds))
                            logging.info(
                                "press Enter key after allowing the data export request to continue..")
                            input()
                            logging.info("trying again.. ({})".format(retry))
                        except errors.TakeoutInvalidError:
                            logging.info("takeout invalidated. delete the session.session file and try again.")
                else:
                    self.client.loop.run_until_complete(self._async(ids, from_id))
        except KeyboardInterrupt as e:
            logging.info("sync cancelled manually")
            raise e
        except:
            raise

    async def _async(self, ids=None, from_id=None):
        """
        Sync syncs messages from Telegram from the last synced message
        into the local SQLite DB.
        """
        _ = await self.client.get_dialogs()
        chat_queue = asyncio.Queue()
        msg_queue = asyncio.Queue(16)
        media_queue = asyncio.Queue(4)
        for group in self.config["groups"]:
            chat_queue.put_nowait((group, from_id))
        
        pending_msgs = self.db.get_pending_messages()
        for chat_id, message_id in pending_msgs:
            msg_queue.put_nowait(self.client.get_messages(self.client.get_entity(chat_id), ids=message_id))
        
        group_workers = [worker.GroupWorker(msg_queue, chat_queue, self.client, self.db)] * len(self.config["groups"])
        msg_workers = [worker.MessageWorker(media_queue, msg_queue, self.client, self.db, self.config)] * 8
        media_workers = [worker.MediaWorker(media_queue, self.client, self.db, self.media_dir, self.media_tmp_dir)] * 2
        tasks = []
        for w in group_workers:
            tasks.append(asyncio.create_task(w.run()))
        for w in msg_workers:
            tasks.append(asyncio.create_task(w.run()))
        for w in media_workers:
            tasks.append(asyncio.create_task(w.run()))
        await asyncio.gather(*tasks, return_exceptions=True)
        await msg_queue.join()
        await media_queue.join()
        await chat_queue.join()

    def new_client(self, session, config):
        if "proxy" in config and config["proxy"].get("enable"):
            proxy = config["proxy"]
            client = TelegramClient(session, config["api_id"], config["api_hash"], proxy=(proxy["protocol"], proxy["addr"], proxy["port"]))
        else:
            client = TelegramClient(session, config["api_id"], config["api_hash"])
        # hide log messages
        # upstream issue https://github.com/LonamiWebs/Telethon/issues/3840
        client_logger = client._log["telethon.client.downloads"]
        client_logger._info = client_logger.info

        def patched_info(*args, **kwargs):
            if (
                args[0] == "File lives in another DC" or
                args[0] == "Starting direct file download in chunks of %d at %d, stride %d"
            ):
                return client_logger.debug(*args, **kwargs)
            client_logger._info(*args, **kwargs)
        client_logger.info = patched_info
        return client

    async def finish_takeout(self):
        await self.client.end_takeout(success=True)

    async def _get_message(self, msg: telethon.tl.custom.Message) -> Tuple[Message, Media, Poll]:
        # https://docs.telethon.dev/en/latest/quick-references/objects-reference.html#message
        if msg is None:
            return (None, None, None)
        # Media.
        sticker = None
        media_id = None
        if msg.media:
            # If it's a sticker, get the alt value (unicode emoji).
            if isinstance(msg.media, telethon.tl.types.MessageMediaDocument) and \
                    hasattr(msg.media, "document") and \
                    msg.media.document.mime_type == "application/x-tgsticker":
                alt = [a.alt for a in msg.media.document.attributes if isinstance(
                    a, telethon.tl.types.DocumentAttributeSticker)]
                if len(alt) > 0:
                    sticker = alt[0]
            elif isinstance(msg.media, telethon.tl.types.MessageMediaPoll):
                poll = self._make_poll(msg)
                self.db.insert_poll(poll)
                media_id = 0
            elif isinstance(msg.media, telethon.tl.types.MessageMediaWebPage) and \
                not isinstance(msg.media.webpage, telethon.tl.types.WebPageEmpty):
                webpage = WebPage(
                    chat_id=msg.chat_id,
                    message_id=msg.id,
                    url=msg.media.webpage.url,
                    title=msg.media.webpage.title,
                    description=msg.media.webpage.description if msg.media.webpage.description else None
                )
                media_id = 1
                self.db.insert_webpage(webpage)
            if self.config["download_media"] and \
                isinstance(msg.media, (telethon.tl.types.MessageMediaPhoto,
                                       telethon.tl.types.MessageMediaDocument,
                                       telethon.tl.types.MessageMediaContact)):
                med = await self._get_media(msg)
                self.db.insert_media(med)
                media_id = med.id
            else:
                logging.info("unknown media type: {}".format(msg.media))

        # Message.
        typ = "message"
        if msg.action:
            if isinstance(msg.action, telethon.tl.types.MessageActionChatAddUser):
                typ = "user_joined"
            elif isinstance(msg.action, telethon.tl.types.MessageActionChatJoinedByLink):
                typ = "user_joined_by_link"
            elif isinstance(msg.action, telethon.tl.types.MessageActionChatDeleteUser):
                typ = "user_left"

        return Message(
            type=typ,
            id=msg.id,
            date=msg.date,
            edit_date=msg.edit_date,
            content=sticker if sticker else msg.raw_text,
            reply_to=msg.reply_to_msg_id if msg.reply_to and msg.reply_to.reply_to_msg_id else None,
            user=await self._get_user(await msg.get_sender(), await msg.get_chat()),
            media_id=media_id
        )

    async def _get_user(self, u, chat) -> User:
        tags = []

        # if user info is empty, check for message from group
        if (
            u is None and
            chat is not None and
            chat.title != ''
            ):
                tags.append("group_self")
                avatar = await self._downloadAvatarForUserOrChat(chat)
                return User(
                    id=chat.id,
                    username=chat.title,
                    first_name=None,
                    last_name=None,
                    tags=tags,
                    avatar=avatar
                )

        is_normal_user = isinstance(u, telethon.tl.types.User)

        if isinstance(u, telethon.tl.types.ChannelForbidden):
            return User(
                id=u.id,
                username=u.title,
                first_name=None,
                last_name=None,
                tags=tags,
                avatar=None
            )

        if is_normal_user:
            if u.bot:
                tags.append("bot")

        if u.scam:
            tags.append("scam")

        if u.fake:
            tags.append("fake")

        # Download sender's profile photo if it's not already cached.
        avatar = await self._downloadAvatarForUserOrChat(u)

        return User(
            id=u.id,
            username=u.username if u.username else str(u.id),
            first_name=u.first_name if is_normal_user else None,
            last_name=u.last_name if is_normal_user else None,
            tags=tags,
            avatar=avatar
        )

    def _make_poll(self, msg) -> Poll:
        if not msg.media.results or not msg.media.results.results:
            return None

        options = [{"label": a.text.text, "count": 0, "correct": False}
                   for a in msg.media.poll.answers]

        total = msg.media.results.total_voters
        if msg.media.results.results:
            for i, r in enumerate(msg.media.results.results):
                options[i]["count"] = r.voters
                options[i]["percent"] = r.voters / \
                    total * 100 if total > 0 else 0
                options[i]["correct"] = r.correct
        logging.info("poll options: {}".format(options))
        return Poll(
            chat_id=msg.chat_id,
            message_id=msg.id,
            title=msg.media.poll.question.text,
            description=json.dumps(options)
        )

    async def _get_media(self, msg) -> Media:
        # Filter by extensions?
        if len(self.config["media_mime_types"]) > 0:
            if hasattr(msg, "file") and hasattr(msg.file, "mime_type") and msg.file.mime_type:
                if msg.file.mime_type not in self.config["media_mime_types"]:
                    logging.info(
                        "skipping media #{} / {}".format(msg.file.name, msg.file.mime_type))
                    return

        try:
            media_id = utils.get_media_id(msg)                    
            logging.info("checking media id: {}, name: {} in cache".format(media_id, msg.file.name))
            if media_id is None:
                raise
            cache = self.db.get_media(media_id, None)
            if cache is not None:
                logging.info("found media id: {} in cache".format(media_id))
                return cache
            logging.info("downloading media id: {} from msg id: {}".format(media_id, msg.id))
            basename, fname, thumb = await self._download_media(msg)
            return Media(
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
            return await self._get_media(msg)
        except (errors.FilerefUpgradeNeededError, errors.FileReferenceExpiredError) as e:
            msg = self.client.get_messages(await msg.get_input_chat(), ids=msg.id)
            return await self._get_media(msg)
        except Exception as e:
            logging.error(
                "error downloading media: #{}: {}".format(msg.id, e))
            traceback.print_exc()

    async def _download_with_progress(self, msg, rename_prefix="", **kwargs):
        def progress_callback(current, total):
            if total is not None:
                pbar.total = total
            pbar.update(current - pbar.n)
        
        def progress_callback2(diff, total):
            if total is not None:
                pbar.total = total
            pbar.update(diff)

        with logging_redirect_tqdm():
            with tqdm(desc=msg.file.name, total=msg.file.size, unit='B', unit_scale=True, unit_divisor=1024, miniters=1) as pbar:
                tmpfile_path = await self.downloader.download(msg, download_folder=self.media_tmp_dir, filename=msg.file.name, progress_callback=progress_callback2, **kwargs)
                basename = os.path.basename(tmpfile_path)
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
                shutil.move(tmpfile_path, destination_path)
                return basename, os.path.basename(destination_path)
            
    async def _async_download(self, msg, rename_prefix=""):
        async def _download(msg, tmpfile_path):
            with open(tmpfile_path, 'wb') as fd:
                with logging_redirect_tqdm():
                    with tqdm(desc=msg.file.name, total=msg.file.size, unit='B', unit_scale=True, unit_divisor=1024, miniters=1) as pbar:
                        async for chunk in self.client.iter_download(msg):
                            # logging.info("chunk: {}".format(chunk))
                            fd.write(chunk)
                            pbar.update(len(chunk))
        tmpfile_path = os.path.join(self.media_tmp_dir, msg.file.name)
        await _download(msg, tmpfile_path)
        basename = os.path.basename(tmpfile_path)
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
        shutil.move(tmpfile_path, destination_path)
        return basename, os.path.basename(destination_path)

    async def _download_media(self, msg) -> [str, str, str]:
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

    async def _download_avatar(self, user):
        fname = "avatar_{}.jpg".format(user.id)
        fpath = os.path.join(self.config["media_dir"], fname)

        if os.path.exists(fpath):
            return fname

        logging.info("downloading avatar #{}".format(user.id))

        # Download the file into a container, resize it, and then write to disk.
        b = BytesIO()
        profile_photo = await self.client.download_profile_photo(user, file=b)
        if profile_photo is None:
            logging.info("user has no avatar #{}".format(user.id))
            return None

        im = Image.open(b)
        im.thumbnail(self.config["avatar_size"], Image.LANCZOS)
        im.save(fpath, "JPEG")

        return fname
    
    async def _get_group_entity(self, group):
        # Get all dialogs for the authorized user, which also
        # syncs the entity cache to get latest entities
        # ref: https://docs.telethon.dev/en/latest/concepts/entities.html#getting-entities
        try:
            # If the passed group is a group ID, extract it.
            group = int(group)
        except ValueError:
            # Not a group ID, we have either a group name or
            # a group username: @group-username
            pass

        try:
            entity = await self.client.get_entity(group)
        except ValueError:
            logging.critical("the group: {} does not exist,"
                             " or the authorized user is not a participant!".format(group))
            # This is a critical error, so exit with code: 1
            exit(1)

        return entity

    async def _downloadAvatarForUserOrChat(self, entity):
        avatar = None
        if self.config["download_avatars"]:
            try:
                fname = await self._download_avatar(entity)
                avatar = fname
            except Exception as e:
                logging.error(
                    "error downloading avatar: #{}: {}".format(entity.id, e))
        return avatar
