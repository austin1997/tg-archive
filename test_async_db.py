#!/usr/bin/env python3
"""
Test script to verify async database operations work correctly.
"""

import asyncio
import os
import tempfile
from tgarchive.db import AsyncDB, DB, User, Message, Media

async def test_async_db():
    """Test the async database operations."""
    
    # Create a temporary database file
    with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as tmp:
        dbfile = tmp.name
    
    try:
        # Test AsyncDB directly
        print("Testing AsyncDB...")
        async with AsyncDB(dbfile) as db:
            # Test creating a chat table
            await db.create_chat_table(12345, "Test Chat")
            
            # Test inserting a user
            user = User(
                id=123,
                username="testuser",
                first_name="Test",
                last_name="User",
                tags=["test"],
                avatar=None
            )
            await db.insert_user(user)
            
            # Test inserting a message
            message = Message(
                id=1,
                type="message",
                date=None,  # Will be set by the database
                edit_date=None,
                content="Hello, world!",
                reply_to=None,
                user=user,
                media_id=None
            )
            await db.insert_message(12345, message)
            
            # Test getting messages
            messages = await db.get_messages(2024, 1)
            print(f"Retrieved {len(messages)} messages")
            
            # Test getting timeline
            timeline = await db.get_timeline()
            print(f"Timeline has {len(timeline)} months")
            
            # Test getting message count
            count = await db.get_message_count(2024, 1)
            print(f"Message count: {count}")
            
            await db.commit()
        
        # Test the synchronous wrapper
        print("\nTesting DB wrapper...")
        db = DB(dbfile)
        
        # Test synchronous operations
        timeline = db.get_timeline()
        print(f"Sync timeline has {len(timeline)} months")
        
        messages = db.get_messages(2024, 1)
        print(f"Sync retrieved {len(messages)} messages")
        
        count = db.get_message_count(2024, 1)
        print(f"Sync message count: {count}")
        
        print("\nAll tests passed!")
        
    finally:
        # Clean up
        if os.path.exists(dbfile):
            os.unlink(dbfile)

if __name__ == "__main__":
    asyncio.run(test_async_db()) 