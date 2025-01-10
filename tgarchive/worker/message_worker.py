import asyncio
import logging
import json
import os
from io import BytesIO
from PIL import Image
import telethon
from telethon import TelegramClient, errors
import telethon.tl.custom
import telethon.tl.types
from tgarchive import db, utils

class MessageWorker:
    def __init__(self, downloader_queue: asyncio.Queue, input_queue: asyncio.Queue, client: TelegramClient, database: db.DB, config: dict):
        self.downloader_queue = downloader_queue
        self.input_queue = input_queue
        self.client = client
        self.db = database
        self.config = config
    
    async def run(self):
        while True:
            msg: telethon.tl.custom.Message = await self.input_queue.get()
            if msg is None:
                break
            chat_id = msg.chat_id
            message = await self._get_message(msg)

            # Insert the records into DB.
            self.db.insert_user(message.user)
            self.db.insert_message(chat_id, message)

            self.db.commit()

    async def _get_message(self, msg: telethon.tl.custom.Message) -> db.Message:
        # https://docs.telethon.dev/en/latest/quick-references/objects-reference.html#message

        # Message.
        typ = "message"
        if msg.action:
            if isinstance(msg.action, telethon.tl.types.MessageActionChatAddUser):
                typ = "user_joined"
            elif isinstance(msg.action, telethon.tl.types.MessageActionChatJoinedByLink):
                typ = "user_joined_by_link"
            elif isinstance(msg.action, telethon.tl.types.MessageActionChatDeleteUser):
                typ = "user_left"

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
                webpage = db.WebPage(
                    chat_id=msg.chat_id,
                    message_id=msg.id,
                    url=msg.media.webpage.url,
                    title=msg.media.webpage.title,
                    description=msg.media.webpage.description if msg.media.webpage.description else None
                )
                media_id = 1
                self.db.insert_webpage(webpage)
            elif self.config["download_media"] and \
                isinstance(msg.media, (telethon.tl.types.MessageMediaPhoto,
                                       telethon.tl.types.MessageMediaDocument,
                                       telethon.tl.types.MessageMediaContact)):
                media_id = await self._get_media(msg)
                self.db.insert_pending_message(msg.chat_id, msg.id)
                await self.downloader_queue.put(msg)
            else:
                logging.info("unknown media type: {}".format(msg.media))

        return db.Message(
            type=typ,
            id=msg.id,
            date=msg.date,
            edit_date=msg.edit_date,
            content=sticker if sticker else msg.raw_text,
            reply_to=msg.reply_to_msg_id if msg.reply_to and msg.reply_to.reply_to_msg_id else None,
            user=await self._get_user(await msg.get_sender(), await msg.get_chat()),
            media_id=media_id
        )

    async def _get_media(self, msg: telethon.tl.custom.Message):
        # Filter by extensions?
        if len(self.config["media_mime_types"]) > 0:
            if hasattr(msg, "file") and hasattr(msg.file, "mime_type") and msg.file.mime_type:
                if msg.file.mime_type not in self.config["media_mime_types"]:
                    logging.info(
                        "skipping media #{} / {}".format(msg.file.name, msg.file.mime_type))
                    return None
        return utils.get_media_id(msg)

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

    async def _get_user(self, u, chat) -> db.User:
        tags = []

        # if user info is empty, check for message from group
        if (
            u is None and
            chat is not None and
            chat.title != ''
            ):
                tags.append("group_self")
                avatar = await self._downloadAvatarForUserOrChat(chat)
                return db.User(
                    id=chat.id,
                    username=chat.title,
                    first_name=None,
                    last_name=None,
                    tags=tags,
                    avatar=avatar
                )

        is_normal_user = isinstance(u, telethon.tl.types.User)

        if isinstance(u, telethon.tl.types.ChannelForbidden):
            return db.User(
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

        return db.User(
            id=u.id,
            username=u.username if u.username else str(u.id),
            first_name=u.first_name if is_normal_user else None,
            last_name=u.last_name if is_normal_user else None,
            tags=tags,
            avatar=avatar
        )

    def _make_poll(self, msg: telethon.tl.custom.Message) -> db.Poll:
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

        return db.Poll(
            chat_id=msg.chat_id,
            message_id=msg.id,
            title=msg.media.poll.question.text,
            description=json.dumps(options)
        )
