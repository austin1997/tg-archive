import json
import math
import os
import aiosqlite
from collections import namedtuple
from datetime import datetime
import pytz
from typing import Iterator, List, Optional, Tuple
import asyncio

create_chat_collection_schema = """
CREATE table IF NOT EXISTS chat (
    id INTEGER NOT NULL PRIMARY KEY,
    title TEXT
);
"""

create_pending_msg_schema = """
CREATE table IF NOT EXISTS pending_msg (
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    PRIMARY KEY (chat_id, message_id)
);
"""

create_chat_schema = """
CREATE table IF NOT EXISTS "{}" (
    id INTEGER NOT NULL,
    type TEXT NOT NULL,
    date TIMESTAMP,
    edit_date TIMESTAMP,
    content TEXT,
    reply_to INTEGER,
    user_id INTEGER,
    media_id INTEGER,
    FOREIGN KEY(user_id) REFERENCES users(id),
    FOREIGN KEY(media_id) REFERENCES media(id),
    PRIMARY KEY (id, reply_to)
);
"""
create_user_schema = """
CREATE table IF NOT EXISTS users (
    id INTEGER NOT NULL PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    tags TEXT,
    avatar TEXT
);
"""
create_media_schema = """
CREATE table IF NOT EXISTS media (
    id INTEGER NOT NULL PRIMARY KEY,
    type TEXT,
    url TEXT,
    title TEXT,
    description TEXT,
    thumb TEXT
);
"""

create_poll_schema = """
CREATE table IF NOT EXISTS poll (
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    title TEXT,
    description TEXT,
    PRIMARY KEY (chat_id, message_id)
);
"""

create_webpage_schema = """
CREATE table IF NOT EXISTS webpage (
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    url TEXT,
    title TEXT,
    description TEXT,
    PRIMARY KEY (chat_id, message_id)
);
"""

User = namedtuple(
    "User", ["id", "username", "first_name", "last_name", "tags", "avatar"])

Message = namedtuple(
    "Message", ["id", "type", "date", "edit_date", "content", "reply_to", "user", "media_id"])

Media = namedtuple(
    "Media", ["id", "type", "url", "title", "description", "thumb"])

Poll = namedtuple("Poll", ["chat_id", "message_id", "title", "description"])

WebPage = namedtuple("WebPage", ["chat_id", "message_id", "url", "title", "description"])

Month = namedtuple("Month", ["date", "slug", "label", "count"])

Day = namedtuple("Day", ["date", "slug", "label", "count", "page"])


def _page(n, multiple):
    return math.ceil(n / multiple)


class AsyncDB:
    tz = None

    def __init__(self, dbfile, tz=None):
        self.dbfile = dbfile
        self.conn = None
        if tz:
            self.tz = pytz.timezone(tz)

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Commit changes on successful exit, then close the connection."""
        if self.conn:
            if exc_type is None:
                # If there was no exception, commit the transaction.
                await self.conn.commit()
            else:
                # An exception occurred, so we don't want to save changes.
                # aiosqlite will roll back automatically on close.
                pass
            await self.close()

    async def connect(self):
        """Initialize the SQLite DB connection."""
        is_new = not os.path.isfile(self.dbfile)
        
        self.conn = await aiosqlite.connect(self.dbfile)
        await self.conn.create_function("PAGE", 2, _page)

        # Create tables if needed
        await self.conn.execute(create_pending_msg_schema)
        await self.conn.execute(create_webpage_schema)
        await self.conn.execute(create_poll_schema)
        await self.conn.execute(create_media_schema)
        await self.conn.execute(create_user_schema)
        await self.conn.execute(create_chat_collection_schema)

    async def close(self):
        """Close the database connection."""
        if self.conn:
            await self.conn.close()
            self.conn = None

    async def print_tabels(self):
        """Print all table names in the database."""
        assert(self.conn)
        async with self.conn.execute("SELECT name FROM sqlite_master WHERE type='table';") as cursor:
            rows = await cursor.fetchall()
            print(rows)

    def _parse_date(self, d) -> str:
        return datetime.strptime(d, "%Y-%m-%dT%H:%M:%S%z")

    async def create_chat_table(self, chat_id: int, title: str):
        """Create a chat table and insert/update chat info."""
        assert(self.conn)
        await self.conn.execute(create_chat_schema.format(chat_id))
        await self.conn.execute("""INSERT INTO chat (id, title)
            VALUES(?, ?) ON CONFLICT (id)
            DO UPDATE SET title=excluded.title
            """, (chat_id, title))

    async def get_last_message_id(self, chat_id: int) -> Tuple[int, Optional[datetime]]:
        """Get the last message ID and date for a chat."""
        assert(self.conn)
        async with self.conn.execute("""
            SELECT id, strftime('%Y-%m-%d 00:00:00', date) as "[timestamp]" FROM "{}"
            WHERE date IS NOT NULL
            ORDER BY id DESC LIMIT 1
        """.format(chat_id)) as cursor:
            res = await cursor.fetchone()
            if not res:
                return 0, None

            id, date = res
            return id, date

    async def get_timeline(self) -> List[Month]:
        """
        Get the list of all unique yyyy-mm month groups and
        the corresponding message counts per period in chronological order.
        """
        assert(self.conn)
        months = []
        async with self.conn.execute("""
            SELECT strftime('%Y-%m-%d 00:00:00', date) as "[timestamp]",
            COUNT(*) FROM messages AS count
            GROUP BY strftime('%Y-%m', date) ORDER BY date
        """) as cursor:
            async for r in cursor:
                date = pytz.utc.localize(r[0])
                if self.tz:
                    date = date.astimezone(self.tz)

                months.append(Month(date=date,
                        slug=date.strftime("%Y-%m"),
                        label=date.strftime("%b %Y"),
                        count=r[1]))
        return months

    async def get_dayline(self, year, month, limit=500) -> List[Day]:
        """
        Get the list of all unique yyyy-mm-dd days corresponding
        message counts and the page number of the first occurrence of 
        the date in the pool of messages for the whole month.
        """
        assert(self.conn)
        days = []
        async with self.conn.execute("""
            SELECT strftime("%Y-%m-%d 00:00:00", date) AS "[timestamp]",
            COUNT(*), PAGE(rank, ?) FROM (
                SELECT ROW_NUMBER() OVER() as rank, date FROM messages
                WHERE strftime('%Y%m', date) = ? ORDER BY id
            )
            GROUP BY "[timestamp]";
        """, (limit, "{}{:02d}".format(year, month))) as cursor:
            async for r in cursor:
                date = pytz.utc.localize(r[0])
                if self.tz:
                    date = date.astimezone(self.tz)

                days.append(Day(date=date,
                      slug=date.strftime("%Y-%m-%d"),
                      label=date.strftime("%d %b %Y"),
                      count=r[1],
                      page=r[2]))
        return days

    async def get_messages(self, year, month, last_id=0, limit=500) -> List[Message]:
        """Get messages for a specific year/month with pagination."""
        assert(self.conn)
        date = "{}{:02d}".format(year, month)
        messages = []

        async with self.conn.execute("""
            SELECT messages.id, messages.type, messages.date, messages.edit_date,
            messages.content, messages.reply_to, messages.user_id,
            users.username, users.first_name, users.last_name, users.tags, users.avatar,
            media.id, media.type, media.url, media.title, media.description, media.thumb
            FROM messages
            LEFT JOIN users ON (users.id = messages.user_id)
            LEFT JOIN media ON (media.id = messages.media_id)
            WHERE strftime('%Y%m', date) = ?
            AND messages.id > ? ORDER by messages.id LIMIT ?
            """, (date, last_id, limit)) as cursor:
            async for r in cursor:
                messages.append(self._make_message(r))
        return messages

    async def get_media(self, media_id: int) -> Optional[Media]:
        """Get media by ID."""
        assert(self.conn)
        async with self.conn.execute("""
                SELECT id, type, url, title, description, thumb
                FROM media
                WHERE id = ?
                """, (media_id,)) as cursor:
            res = await cursor.fetchone()
            if res is None:
                return None
            media_id, media_type, media_url, media_title, desc, media_thumb = res
            return Media(id=media_id,
                        type=media_type,
                        url=media_url,
                        title=media_title,
                        description=desc,
                        thumb=media_thumb)

    async def get_message_count(self, year, month) -> int:
        """Get message count for a specific year/month."""
        assert(self.conn)
        date = "{}{:02d}".format(year, month)

        async with self.conn.execute("""
            SELECT COUNT(*) FROM messages WHERE strftime('%Y%m', date) = ?
            """, (date,)) as cursor:
            total, = await cursor.fetchone()
            return total

    async def insert_user(self, u: User):
        """Insert a user and if they exist, update the fields."""
        assert(self.conn)
        await self.conn.execute("""INSERT INTO users (id, username, first_name, last_name, tags, avatar)
            VALUES(?, ?, ?, ?, ?, ?) ON CONFLICT (id)
            DO UPDATE SET username=excluded.username, first_name=excluded.first_name,
                last_name=excluded.last_name, tags=excluded.tags, avatar=excluded.avatar
            """, (u.id, u.username, u.first_name, u.last_name, " ".join(u.tags), u.avatar))

    async def insert_media(self, m: Media):
        """Insert media record."""
        assert(self.conn)
        await self.conn.execute("""INSERT OR IGNORE INTO media
            (id, type, url, title, description, thumb)
            VALUES(?, ?, ?, ?, ?, ?)""",
                    (m.id,
                    m.type,
                    m.url,
                    m.title,
                    m.description,
                    m.thumb)
                    )

    async def insert_poll(self, p: Poll):
        """Insert poll record."""
        assert(self.conn)
        await self.conn.execute("""INSERT OR REPLACE INTO poll
            (chat_id, message_id, title, description)
            VALUES(?, ?, ?, ?)""",
                    (p.chat_id,
                    p.message_id,
                    p.title,
                    p.description)
                    )
    
    async def insert_webpage(self, w: WebPage):
        """Insert webpage record."""
        assert(self.conn)
        await self.conn.execute("""INSERT OR REPLACE INTO webpage
            (chat_id, message_id, url, title, description)
            VALUES(?, ?, ?, ?, ?)""",
                    (w.chat_id,
                    w.message_id,
                    w.url,
                    w.title,
                    w.description)
                    )

    async def insert_message(self, chat_id: int, m: Message):
        """Insert message record. Only update date and edit_date if primary key exists."""
        assert(self.conn)
        sql = f"""INSERT INTO "{chat_id}"
            (id, type, date, edit_date, content, reply_to, user_id, media_id)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id, reply_to) DO UPDATE SET
                date=excluded.date,
                edit_date=excluded.edit_date
        """
        params = (
            m.id,
            m.type,
            m.date.strftime("%Y-%m-%d %H:%M:%S"),
            m.edit_date.strftime("%Y-%m-%d %H:%M:%S") if m.edit_date else None,
            m.content,
            m.reply_to,
            m.user.id,
            m.media_id
        )
        await self.conn.execute(sql, params)
    
    async def insert_pending_message(self, chat_id: int, message_id: int):
        """Insert pending message record."""
        assert(self.conn)
        await self.conn.execute("""INSERT OR REPLACE INTO pending_msg
            (chat_id, message_id)
            VALUES(?, ?)""", (chat_id, message_id))
    
    async def remove_pending_message(self, chat_id: int, message_id: int):
        """Remove pending message record."""
        assert(self.conn)
        await self.conn.execute("""DELETE FROM pending_msg
            WHERE chat_id = ? AND message_id = ?""", (chat_id, message_id))
    
    async def get_pending_messages(self) -> List[Tuple[int, int]]:
        """Get all pending messages."""
        assert(self.conn)
        async with self.conn.execute("""
            SELECT chat_id, message_id FROM pending_msg
        """) as cursor:
            return await cursor.fetchall()

    async def commit(self):
        """Commit pending writes to the DB."""
        assert(self.conn)
        await self.conn.commit()

    def _make_message(self, m) -> Message:
        """Makes a Message() object from an SQL result tuple."""
        id, typ, date, edit_date, content, reply_to, \
            user_id, username, first_name, last_name, tags, avatar, \
            media_id, media_type, media_url, media_title, media_description, media_thumb = m

        md = None
        if media_id:
            desc = media_description
            if media_type == "poll":
                desc = json.loads(media_description)

            md = Media(id=media_id,
                       type=media_type,
                       url=media_url,
                       title=media_title,
                       description=desc,
                       thumb=media_thumb)

        date = pytz.utc.localize(date) if date else None
        edit_date = pytz.utc.localize(edit_date) if edit_date else None

        if self.tz:
            date = date.astimezone(self.tz) if date else None
            edit_date = edit_date.astimezone(self.tz) if edit_date else None

        return Message(id=id,
                       type=typ,
                       date=date,
                       edit_date=edit_date,
                       content=content,
                       reply_to=reply_to,
                       user=User(id=user_id,
                                 username=username,
                                 first_name=first_name,
                                 last_name=last_name,
                                 tags=tags,
                                 avatar=avatar),
                       media=md)


# Backward compatibility - keep the old DB class for existing code
class DB:
    """Synchronous wrapper around AsyncDB for backward compatibility."""
    
    def __init__(self, dbfile, tz=None):
        self.async_db = AsyncDB(dbfile, tz)
        self._loop = None
        self._conn = None

    def _get_loop(self):
        """Get or create event loop."""
        if self._loop is None:
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                self._loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._loop)
        return self._loop

    def _run_async(self, coro):
        """Run async coroutine in sync context."""
        loop = self._get_loop()
        if loop.is_running():
            # If we're already in an async context, we need to create a task
            # This is a bit of a hack, but it works for the current use case
            future = asyncio.create_task(coro)
            return future
        else:
            return loop.run_until_complete(coro)

    def __getattr__(self, name):
        """Delegate attribute access to async_db with async-to-sync conversion."""
        if hasattr(self.async_db, name):
            attr = getattr(self.async_db, name)
            if asyncio.iscoroutinefunction(attr):
                def sync_wrapper(*args, **kwargs):
                    return self._run_async(attr(*args, **kwargs))
                return sync_wrapper
            return attr
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

    def print_tabels(self):
        """Synchronous version of print_tabels."""
        return self._run_async(self.async_db.print_tabels())

    def create_chat_table(self, chat_id: int, title: str):
        """Synchronous version of create_chat_table."""
        return self._run_async(self.async_db.create_chat_table(chat_id, title))

    def get_last_message_id(self, chat_id: int) -> Tuple[int, Optional[datetime]]:
        """Synchronous version of get_last_message_id."""
        return self._run_async(self.async_db.get_last_message_id(chat_id))

    def get_timeline(self) -> List[Month]:
        """Synchronous version of get_timeline."""
        return self._run_async(self.async_db.get_timeline())

    def get_dayline(self, year, month, limit=500) -> List[Day]:
        """Synchronous version of get_dayline."""
        return self._run_async(self.async_db.get_dayline(year, month, limit))

    def get_messages(self, year, month, last_id=0, limit=500) -> List[Message]:
        """Synchronous version of get_messages."""
        return self._run_async(self.async_db.get_messages(year, month, last_id, limit))

    def get_media(self, media_id: int) -> Optional[Media]:
        """Synchronous version of get_media."""
        return self._run_async(self.async_db.get_media(media_id))

    def get_message_count(self, year, month) -> int:
        """Synchronous version of get_message_count."""
        return self._run_async(self.async_db.get_message_count(year, month))

    def insert_user(self, u: User):
        """Synchronous version of insert_user."""
        return self._run_async(self.async_db.insert_user(u))

    def insert_media(self, m: Media):
        """Synchronous version of insert_media."""
        return self._run_async(self.async_db.insert_media(m))

    def insert_poll(self, p: Poll):
        """Synchronous version of insert_poll."""
        return self._run_async(self.async_db.insert_poll(p))

    def insert_webpage(self, w: WebPage):
        """Synchronous version of insert_webpage."""
        return self._run_async(self.async_db.insert_webpage(w))

    def insert_message(self, chat_id: int, m: Message):
        """Synchronous version of insert_message."""
        return self._run_async(self.async_db.insert_message(chat_id, m))

    def insert_pending_message(self, chat_id: int, message_id: int):
        """Synchronous version of insert_pending_message."""
        return self._run_async(self.async_db.insert_pending_message(chat_id, message_id))

    def remove_pending_message(self, chat_id: int, message_id: int):
        """Synchronous version of remove_pending_message."""
        return self._run_async(self.async_db.remove_pending_message(chat_id, message_id))

    def get_pending_messages(self) -> List[Tuple[int, int]]:
        """Synchronous version of get_pending_messages."""
        return self._run_async(self.async_db.get_pending_messages())

    def commit(self):
        """Synchronous version of commit."""
        return self._run_async(self.async_db.commit())
