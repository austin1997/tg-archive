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
cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='1385489711';")
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
            date TIMESTAMP,
            edit_date TIMESTAMP,
            content TEXT,
            reply_to INTEGER NOT NULL DEFAULT -1,
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
conn.commit()
print("Done")

# %%
cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
print(cur.fetchall())

# %%
# Find the ids that have multiple NULL reply_to entries:
query = """
SELECT *
FROM "{}"
WHERE reply_to IS NULL AND id == 211471
"""
cur.execute(query.format("1385489711"))
print(cur.fetchall())
# %%
# Delete duplicates
def delete_duplicates(cur: sqlite3.Cursor, table_name: str):
    template = """
    DELETE FROM "{}"
    WHERE reply_to IS NULL
    AND rowid NOT IN (
        SELECT MIN(rowid) -- Or MAX(rowid), depending on which one you want to keep
        FROM "{}"
        WHERE reply_to IS NULL
        GROUP BY id
        HAVING COUNT(*) > 1
    );
    """
    cur.execute(template.format(table_name, table_name))
    conn.commit()

def update_table(cur: sqlite3.Cursor, table_name: str):
    template = """
    UPDATE "{}"
    SET reply_to = -1
    WHERE reply_to IS NULL;
    """
    cur.execute(template.format(table_name))
    conn.commit()

for id, _ in chats:
    table_name = str(id)
    delete_duplicates(cur, table_name)
    update_table(cur, table_name)
# %%
conn.close()
# %%
