from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# Standard connection string for a local MongoDB server
CONNECTION_STRING = "mongodb://localhost:27017/"

try:
    # Try to create a client
    client = MongoClient(CONNECTION_STRING, serverSelectionTimeoutMS=5000)
    
    # Send a 'ping' command to check if the server is reachable
    client.admin.command('ping')
    print(" Connected to MongoDB!")

except ConnectionFailure as e:
    print(f" MongoDB connection error: {e}")
    print("---")
    print("Make sure MongoDB is installed and running on port 27017.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
