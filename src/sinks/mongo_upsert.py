"""MongoDB sink for upserting aggregated KPIs."""

from pyspark.sql import DataFrame


def setup_mongo_sink(df: DataFrame, mongo_uri: str, mongo_db: str, mongo_collection: str):
    """Write DataFrame to MongoDB with upsert capability."""
    
    # Convert DataFrame to format suitable for MongoDB
    records = df.collect()
    
    if not records:
        return
    
    try:
        from pymongo import MongoClient
        
        client = MongoClient(mongo_uri)
        db = client[mongo_db]
        collection = db[mongo_collection]
        
        for row in records:
            # Convert Row to dict
            doc = row.asDict()
            
            # Convert minute timestamp to string for MongoDB
            if 'minute' in doc and doc['minute']:
                doc['minute'] = doc['minute'].isoformat()
            
            # Upsert based on minute timestamp
            collection.replace_one(
                {"minute": doc['minute']},
                doc,
                upsert=True
            )
        
        print(f"✓ Upserted {len(records)} records to MongoDB")
        
    except Exception as e:
        print(f"❌ MongoDB write error: {e}")
        raise
    finally:
        try:
            client.close()
        except:
            pass

# Updated: 2025-10-04 19:46:27
# Added during commit replay


# Updated: 2025-10-04 19:46:33
# Added during commit replay
