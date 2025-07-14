# %%
import os
import sqlite3

dbfile = '/mnt/Truenas/Pool0/ZZH/telegram/archive/test_site/data.sqlite'

conn = sqlite3.Connection(
            dbfile, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
cur = conn.cursor()

# %%
cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
print(cur.fetchall())

# %%
cur.execute('DROP TABLE "1125392550_new";')
print(cur.fetchall())
cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
print(cur.fetchall())

# %%
cur.execute('SELECT * FROM "chat";')
chats = cur.fetchall()
print(chats)

# %%
def create_new_table(cur: sqlite3.Cursor, name: str):
    new_table_template = """
        CREATE table IF NOT EXISTS "{}" (
            id INTEGER NOT NULL,
            type TEXT NOT NULL,
            date TIMESTAMP NOT NULL,
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
    cur.execute(new_table_template.format(name))
    print(cur.fetchall())

def copy_table(cur: sqlite3.Cursor, from_table: str, to_table):
    copy_table_template = """
        INSERT INTO "{}"
        (id, type, date, edit_date, content, reply_to, user_id, media_id)
        SELECT id, type, date, edit_date, content, reply_to, user_id, media_id FROM "{}";
        """
    cur.execute(copy_table_template.format(to_table, from_table))
    print(cur.fetchall())

def drop_table(cur: sqlite3.Cursor, name: str):
    drop_table_template = """
        DROP TABLE "{}";
        """
    cur.execute(drop_table_template.format(name))
    print(cur.fetchall())

def rename_table(cur: sqlite3.Cursor, from_table: str, to_table):
    rename_table_template = """
        ALTER TABLE "{}" RENAME TO "{}";
        """
    cur.execute(rename_table_template.format(from_table, to_table))
    print(cur.fetchall())

for id, _ in chats:
    target_name = str(id)
    temp_name = target_name + "_new"
    create_new_table(cur, temp_name)
    copy_table(cur, target_name, temp_name)
    drop_table(cur, target_name)
    rename_table(cur, temp_name, target_name)

# %%
cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
print(cur.fetchall())
# %%
