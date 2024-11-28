import sys
import os
import pathlib
import time
import datetime as dt
from telethon import utils, client
from telethon.tl import types

from .FastTelethon import upload_file, download_file

def get_media_id(msg):
    media_id = None
    if getattr(msg, "photo", None) is not None:
        media_id = msg.photo.id
    elif getattr(msg, "document", None) is not None:
        media_id = msg.document.id
    return media_id

def get_photo_location(photo, thumb = None):
    """Specialized version of .download_media() for photos"""
    # Determine the photo and its largest size
    if isinstance(photo, types.MessageMediaPhoto):
        photo = photo.photo
        dc_id = photo.dc_id
    if not isinstance(photo, types.Photo):
        return None

    # Include video sizes here (but they may be None so provide an empty list)
    size = client.downloads.DownloadMethods._get_thumb(photo.sizes + (photo.video_sizes or []), thumb)
    if not size or isinstance(size, types.PhotoSizeEmpty):
        return None

    # if isinstance(size, (types.PhotoCachedSize, types.PhotoStrippedSize)):
    #     return self._download_cached_photo_size(size, file)

    if isinstance(size, types.PhotoSizeProgressive):
        file_size = max(size.sizes)
    else:
        file_size = size.size


    return dc_id, types.InputPhotoFileLocation(
        id=photo.id,
        access_hash=photo.access_hash,
        file_reference=photo.file_reference,
        thumb_size=size.type
    ), file_size

def get_document_location(document):
    """Specialized version of .download_media() for documents."""
    if isinstance(document, types.MessageMediaDocument):
        dc_id, location = utils.get_input_location(document)
        return dc_id, location, document.document.size
    else:
        return None

class Timer:
    def __init__(self, time_between=5):
        self.start_time = time.time()
        self.time_between = time_between

    def can_send(self):
        if time.time() > (self.start_time + self.time_between):
            self.start_time = time.time()
            return True
        return False

def progress_bar_str(done, total):
    percent = round(done/total*100, 2)
    strin = "░░░░░░░░░░"
    strin = list(strin)
    for i in range(round(percent)//10):
        strin[i] = "█"
    strin = "".join(strin)
    final = f"Percent: {percent}%\n{human_readable_size(done)}/{human_readable_size(total)}\n{strin}"
    return final 

def human_readable_size(size, decimal_places=2):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if size < 1024.0 or unit == 'PB':
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f} {unit}"

async def fast_download(client, msg, download_folder: str, filename = None, thumb = None, progress_callback = None):
    if msg.document is not None:
        dc_id, location, file_size = get_document_location(msg.document)
    elif msg.photo is not None:
        dc_id, location, file_size = get_photo_location(msg.photo, thumb)
    else:
        return None

    if filename is None:
        filename = msg.file.name
    if filename is None:
        filename = get_media_id(msg) + utils.get_extension(msg.media)

    if os.path.exists(download_folder):
        if os.path.isfile(download_folder):
            filename = download_folder
        else:
            filename = os.path.join(download_folder, filename)
    else:
        return None

    with open(filename, "wb") as f:
        await download_file(
            client=client,
            dc_id=dc_id,
            location=location,
            file_size=file_size,
            out=f,
            progress_callback=progress_callback
        )
    return filename

async def fast_upload(client, file_location, reply=None, name=None, progress_bar_function = progress_bar_str):
    timer = Timer()
    if name == None:
        name = file_location.split("/")[-1]
    async def progress_bar(downloaded_bytes, total_bytes):
        if timer.can_send():
            data = progress_bar_function(downloaded_bytes, total_bytes)
            await reply.edit(f"Uploading...\n{data}")
    if reply != None:
        with open(file_location, "rb") as f:
            the_file = await upload_file(
                client=client,
                file=f,
                name=name,
                progress_callback=progress_bar
            )
    else:
        with open(file_location, "rb") as f:
            the_file = await upload_file(
                client=client,
                file=f,
                name=name,
            )
        
    return the_file