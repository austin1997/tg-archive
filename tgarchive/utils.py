import sys
import os
import pathlib
import time
import datetime as dt
from telethon import utils, client
from telethon.tl import types

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
    dc_id = None
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

# async def fast_upload(client, file_location, reply=None, name=None, progress_bar_function = progress_bar_str):
#     timer = Timer()
#     if name == None:
#         name = file_location.split("/")[-1]
#     async def progress_bar(downloaded_bytes, total_bytes):
#         if timer.can_send():
#             data = progress_bar_function(downloaded_bytes, total_bytes)
#             await reply.edit(f"Uploading...\n{data}")
#     if reply != None:
#         with open(file_location, "rb") as f:
#             the_file = await upload_file(
#                 client=client,
#                 file=f,
#                 name=name,
#                 progress_callback=progress_bar
#             )
#     else:
#         with open(file_location, "rb") as f:
#             the_file = await upload_file(
#                 client=client,
#                 file=f,
#                 name=name,
#             )
        
#     return the_file