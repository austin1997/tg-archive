import asyncio
import logging
from telethon import TelegramClient
from tgarchive import db

class GroupWorker:
    def __init__(self, output_queue: asyncio.Queue, input_queue: asyncio.Queue, client: TelegramClient, database: db.DB):
        self.output_queue = output_queue
        self.input_queue = input_queue
        self.client = client
        self.db = database

    async def run(self):
        while not self.input_queue.empty():
            ids = None
            (group, from_id) = await self.input_queue.get()
            logging.info(f"Handling group {group}")
            group_entity = await self.client.get_entity(group)
            group_id = group_entity.id
            self.db.create_chat_table(group_id, group_entity.title)

            if ids is not None:
                last_id, last_date = (ids, None)
                logging.info("fetching message id={}".format(ids))
            elif from_id is not None:
                last_id, last_date = (from_id, None)
                logging.info("fetching from last message id={}".format(last_id))
            else:
                last_id, last_date = self.db.get_last_message_id(group_id)
                logging.info("fetching from last message id={} ({})".format(
                    last_id, last_date))
            
            n = 0
            async for msg in self.client.iter_messages(group_entity, reverse=True, offset_id=last_id if last_id is not None else 0, ids=ids):
                await self.output_queue.put(msg)
                last_date = msg.date
                n += 1
            logging.info("{} finished. fetched {} messages. last message = {}".format(group_id, n, last_date))
