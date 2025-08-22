#!/usr/bin/env python3
"""
Quick test script to verify MongoDB connection and data insertion
"""
import logging
from pymongo import MongoClient
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MongoDB connection
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "manga_raw_data"

def test_mongodb_connection():
    """Test MongoDB connection and create test collection"""
    try:
        # Connect to MongoDB
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        
        # Test connection
        client.admin.command('ping')
        logger.info("✅ MongoDB connection successful")
        
        # List existing collections
        collections = db.list_collection_names()
        logger.info(f"📋 Existing collections: {collections}")
        
        # Create test collection with sample data
        test_collection = db["test_mal_data"]
        
        # Insert test document
        test_doc = {
            "_id": "mal_test_123",
            "source": "mal",
            "source_id": "123",
            "source_url": "https://myanimelist.net/manga/123",
            "fetched_at": datetime.utcnow(),
            "manga_info": {
                "title": "Test Manga",
                "score": 8.5,
                "status": "Completed"
            },
            "reviews": [
                {
                    "author": "TestUser",
                    "rating": 9,
                    "text": "Great manga!"
                }
            ],
            "recommendations": [
                {
                    "title": "Similar Manga",
                    "mal_id": "456"
                }
            ],
            "status": "ok",
            "http": {"code": 200}
        }
        
        # Insert or update
        result = test_collection.replace_one(
            {"_id": test_doc["_id"]}, 
            test_doc, 
            upsert=True
        )
        
        if result.upserted_id:
            logger.info(f"✅ Inserted new test document: {result.upserted_id}")
        else:
            logger.info(f"✅ Updated existing test document")
        
        # Verify insertion
        count = test_collection.count_documents({})
        logger.info(f"📊 Test collection now has {count} documents")
        
        # List collections again
        collections_after = db.list_collection_names()
        logger.info(f"📋 Collections after test: {collections_after}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ MongoDB test failed: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Testing MongoDB Connection and Data Insertion")
    print("=" * 60)
    
    success = test_mongodb_connection()
    
    if success:
        print("\n✅ MongoDB test completed successfully!")
        print("You should now see 'test_mal_data' collection in your database.")
    else:
        print("\n❌ MongoDB test failed!")
        print("Check if MongoDB is running and connection settings are correct.")
