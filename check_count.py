# check_count.py
from pymongo import MongoClient

try:
    # 1. Connect to MongoDB
    CLIENT = MongoClient("mongodb://localhost:27017/")
    DB = CLIENT["dicom_dw"]
    fact_study_coll = DB["fact_study"]
    
    # Count total documents in fact_study collection
    count = fact_study_coll.count_documents({})
    
    print(f"\n--- ✅ Database Check ---")
    print(f"Total studies in 'fact_study': {count}")
    print(f"---------------------------\n")

except Exception as e:
    print(f"Connection Error: {e}")