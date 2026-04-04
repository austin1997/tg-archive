import json
import os
from io import BytesIO
import traceback
from PIL import Image
import asyncio
import logging
import re

import telethon
import telethon.tl.custom
import telethon.tl.types
from telethon import TelegramClient
from tgarchive import db, utils

class MessageWorker:
    def __init__(self, output_queue: utils.OrderedPriorityQueue, chat_id: int, pending_msgs: asyncio.Queue, client: TelegramClient, database: db.AsyncDB, config: dict, media_dir: str, download_media: bool=True):
        self.output_queue = output_queue
        self.chat_id = chat_id
        self.pending_msgs = pending_msgs
        self.special_users = config.get("special_users", {})
        self.client = client
        self.db = database
        self.config = config
        self.group_entity = None
        self.media_dir = media_dir
        self.download_media = download_media

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

        ids = None
        group = self.chat_id
        from_id = None
        await self.get_group_entity(group)
        if self.group_entity is None:
            logging.warning(f"Group entity is None for group {group}")
            return
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
                logging.info("fetched {} messages in chat {}. last message = {}: {}".format(n, msg.chat_id, msg.id, last_date))
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
                    # raise e

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
        if msg.sender_id == self.special_users.get("flash_photo_cat_bot", 0) and msg.button_count > 0:
            logging.info("Got flash media message")
            sent_msg = await self.client.send_message((await msg.get_sender()), "https://t.me/c/2042004332/" + str(msg.id))
            try:
                await msg.click(text=re.compile(r".*查看.*").match)    
            except Exception as e:
                traceback.print_exc()
                logging.info("Failed to get one flash media")
            await self.db.insert_message_link(msg.chat_id, msg.id, msg.sender_id, sent_msg.id)
        elif msg.sender_id == self.special_users.get("Gooyo_bot", 0) and msg.media is None:
            logging.info("Got Gooyo_bot media None message sender_id: {}".format(msg.sender_id))
            msg = await self.client.get_messages(await msg.get_input_chat(), ids=msg.id)
            if msg is not None and msg.buttons:
                try:
                    await asyncio.sleep(8)
                    page_buttons = []
                    current_page_idx = None
                    for row in msg.buttons:
                        for btn in row:
                            match = re.search(r'\d+', btn.text)
                            if match:
                                page_buttons.append(btn)
                                if match.start() > 0:
                                    current_page_idx = len(page_buttons) - 1

                    if current_page_idx is not None and current_page_idx < len(page_buttons) - 1:
                        next_btn = page_buttons[current_page_idx + 1]
                        await next_btn.click()
                        logging.info("Clicked next page '{}' in Gooyo_bot message_id: {}".format(
                            next_btn.text, msg.id))
                    elif current_page_idx is not None:
                        logging.info("Already on last page in Gooyo_bot message_id: {}".format(msg.id))
                    else:
                        logging.info("Could not find current page button in Gooyo_bot message_id: {}".format(msg.id))
                except Exception as e:
                    traceback.print_exc()
                    logging.info("Failed to click button in Gooyo_bot message_id: {}".format(msg.id))
        elif msg.sender_id == self.special_users.get("atfileslinksbot", 0) and msg.media is None:
            logging.info("Got atfileslinksbot media None message sender_id: {}".format(msg.sender_id))
            msg = await self.client.get_messages(await msg.get_input_chat(), ids=msg.id)
            if msg is not None and msg.buttons:
                try:
                    await msg.click(text=re.compile(r".*加载.*").match)
                    logging.info("Clicked load button in atfileslinksbot message_id: {}".format(msg.id))
                except Exception as e:
                    traceback.print_exc()
                    logging.info("Failed to click load button in atfileslinksbot message_id: {}".format(msg.id))
        elif msg.sender_id == self.special_users.get("QQfile_bot", 0) and msg.media is None:
            logging.info("Got QQfile_bot media None message sender_id: {}".format(msg.sender_id))
            msg = await self.client.get_messages(await msg.get_input_chat(), ids=msg.id)
            if msg is not None and msg.buttons:
                try:
                    await msg.click(text=re.compile(r".*推送全部.*").match)
                    logging.info("Clicked load button in QQfile_bot message_id: {}".format(msg.id))
                except Exception as e:
                    traceback.print_exc()
                    logging.info("Failed to click load button in QQfile_bot message_id: {}".format(msg.id))
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
            elif self.download_media and \
                isinstance(msg.media, (telethon.tl.types.MessageMediaPhoto,
                                       telethon.tl.types.MessageMediaDocument,
                                       telethon.tl.types.MessageMediaContact)):
                media_id = await self._get_media(msg)
                if media_id is None:
                    logging.warning("Got None media_id in chat_id:{}, msg_id: {}".format(msg.chat_id, msg.id))
                elif await self.db.get_media(media_id) is None:
                    await self.db.insert_pending_message(msg.chat_id, msg.id)
                    temp = (msg, self.media_dir)
                    await self.output_queue.put(priority, temp)
                else:
                    logging.info("found media id: {} in cache in chat: {}".format(media_id, msg.chat_id))
            else:
                logging.info("unknown media type: {}".format(msg.media))

        return db.Message(
            type=typ,
            id=msg.id,
            date=None if remove_date else msg.date,
            edit_date=msg.date if remove_date else msg.edit_date,
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
        fpath = os.path.join(self.media_dir, fname)

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
            title=poll.question.text,
            description=json.dumps([a.text.text for a in poll.answers])
        )
