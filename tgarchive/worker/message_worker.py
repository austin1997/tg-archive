import json
import os
from io import BytesIO
import traceback
from PIL import Image
import asyncio
import logging
import telethon
import telethon.tl.custom
import telethon.tl.types
from telethon import TelegramClient
from tgarchive import db, utils

class MessageWorker:
    def __init__(self, output_queue: utils.OrderedPriorityQueue, input_queue: asyncio.Queue, pending_msgs: asyncio.Queue, client: TelegramClient, database: db.AsyncDB, config: dict):
        self.output_queue = output_queue
        self.input_queue = input_queue
        self.pending_msgs = pending_msgs
        self.client = client
        self.db = database
        self.config = config
        self.group_entity = None

    async def get_group_entity(self, group):
        logging.info(f"Handling group {group}")
        # try converting group to int
        try:
            group = int(group)
        except ValueError:
            pass
        try:
            self.group_entity = await self.client.get_entity(group)
        except Exception as e:
            traceback.print_exc()
            logging.error("error getting group entity: #{}: {}".format(group, e))

    async def run(self):
        while not self.pending_msgs.empty():
            message = await self.pending_msgs.get()
            logging.info(f"Processing pending message {message.id} from chat {message.chat_id}")

            await self.handle_message(message, True)

        while not self.input_queue.empty():
            ids = None
            (group, from_id) = await self.input_queue.get()
            await self.get_group_entity(group)
            if self.group_entity is None:
                continue
            group_id = self.group_entity.id
            if isinstance(self.group_entity, telethon.tl.types.Channel):
                await self.db.create_chat_table(group_id, self.group_entity.title)
            else:
                await self.db.create_chat_table(group_id, self.group_entity.username)

            if ids is not None:
                last_id, last_date = (ids, None)
                logging.info("fetching message id={}".format(ids))
            elif from_id is not None:
                last_id, last_date = (from_id, None)
                logging.info("fetching from last message id={}".format(last_id))
            else:
                last_id, last_date = await self.db.get_last_message_id(group_id)
                logging.info("fetching from last message id={} ({})".format(
                    last_id, last_date))
            
            # last_id = None
            n = 0
            async for msg in self.client.iter_messages(self.group_entity, reverse=True, offset_id=last_id if last_id is not None else 0, ids=ids):
                last_date = msg.date
                n += 1
                await self.handle_message(msg, True)
                if n % 1000 == 0:
                    logging.info("fetched {} messages. last message = {}: {}".format(n, msg.id, last_date))
            logging.info("{} finished. fetched {} messages. last message = {}".format(group_id, n, last_date))

    async def handle_message(self, msg: telethon.tl.custom.Message, search_replies: bool, priority: int = 1, remove_date: bool = False):
        if msg is None:
            return
        chat_id = (await msg.get_chat()).id
        message = await self._get_message(msg, priority, remove_date)
        if self.group_entity is not None and isinstance(self.group_entity, telethon.types.Channel) and self.group_entity.broadcast:
            if search_replies:
                try:
                    async for reply in self.client.iter_messages(self.group_entity, reverse=True, reply_to=msg.id):
                        logging.info("fetching replies to message id={} ({}), at chat {}".format(msg.id, reply.id, self.group_entity.title))
                        await self.handle_message(reply, False)
                except (telethon.errors.PeerIdInvalidError,
                        telethon.errors.rpcerrorlist.MsgIdInvalidError):
                    pass
                except Exception as e:
                    logging.error("Error while handling reply message: {}".format(e))
                    raise e

        # Insert the records into DB.
        await self.db.insert_user(message.user)
        await self.db.insert_message(chat_id, message)

        await self.db.commit()

    async def _get_message(self, msg: telethon.tl.custom.Message, priority: int, remove_date: bool = False) -> db.Message:
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
                not isinstance(msg.media.document, telethon.tl.types.DocumentEmpty) and \
                    hasattr(msg.media, "document") and \
                    msg.media.document is not None and \
                    msg.media.document.mime_type == "application/x-tgsticker":
                alt = [a.alt for a in msg.media.document.attributes if isinstance(
                    a, telethon.tl.types.DocumentAttributeSticker)]
                if len(alt) > 0:
                    sticker = alt[0]
            elif isinstance(msg.media, telethon.tl.types.MessageMediaPoll):
                poll = self._make_poll(msg)
                if poll is not None:
                    await self.db.insert_poll(poll)
                else:
                    logging.info(f"poll media in chat_id: {msg.chat_id}, msg_id: {msg.id} disappeared.")
                media_id = 0
            elif isinstance(msg.media, telethon.tl.types.MessageMediaWebPage):
                if isinstance(msg.media.webpage, telethon.tl.types.WebPageEmpty):
                    webpage = db.WebPage(
                        chat_id=msg.chat_id,
                        message_id=msg.id,
                        url=msg.media.webpage.url,
                        title=None,
                        description=None
                    )
                else:
                    webpage = db.WebPage(
                        chat_id=msg.chat_id,
                        message_id=msg.id,
                        url=msg.media.webpage.url,
                        title=msg.media.webpage.title,
                        description=msg.media.webpage.description if msg.media.webpage.description else None
                    )
                media_id = 1
                await self.db.insert_webpage(webpage)
            elif self.config["download_media"] and \
                isinstance(msg.media, (telethon.tl.types.MessageMediaPhoto,
                                       telethon.tl.types.MessageMediaDocument,
                                       telethon.tl.types.MessageMediaContact)):
                media_id = await self._get_media(msg)
                if media_id is None:
                    logging.warning("Got None media_id in chat_id:{}, msg_id: {}".format(msg.chat_id, msg.id))
                if media_id is not None and await self.db.get_media(media_id) is None:
                    await self.db.insert_pending_message(msg.chat_id, msg.id)
                    await self.output_queue.put(priority, msg)
                else:
                    logging.info("found media id: {} in cache".format(media_id))
            else:
                logging.info("unknown media type: {}".format(msg.media))

        return db.Message(
            type=typ,
            id=msg.id,
            date=None if remove_date else msg.date,
            edit_date=msg.edit_date,
            content=sticker if sticker else msg.raw_text,
            reply_to=msg.reply_to_msg_id if msg.reply_to and msg.reply_to.reply_to_msg_id else -1,
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
        try:
            await self.client.download_media(user.photo, b)
            b.seek(0)
            img = Image.open(b)
            img = img.resize(self.config["avatar_size"])
            img.save(fpath, "JPEG")
            return fname
        except Exception as e:
            logging.error("error downloading avatar: #{}: {}".format(user.id, e))
            return None

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
        if not isinstance(msg.media, telethon.tl.types.MessageMediaPoll):
            return None
        poll = msg.media.poll
        if poll is None:
            return None
        return db.Poll(
            chat_id=msg.chat_id,
            message_id=msg.id,
            title=poll.question,
            description=json.dumps([a.text for a in poll.answers])
        )
