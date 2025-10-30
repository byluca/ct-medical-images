from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# La stringa di connessione standard per un server MongoDB locale [cite: 154]
CONNECTION_STRING = "mongodb://localhost:27017/"

try:
    # Tenta di creare un client
    client = MongoClient(CONNECTION_STRING, serverSelectionTimeoutMS=5000)
    
    # Invia un comando 'ping' per verificare che il server sia raggiungibile [cite: 161]
    client.admin.command('ping')
    print("✅ Connessione a MongoDB riuscita!")

except ConnectionFailure as e:
    print(f"❌ Errore di connessione a MongoDB: {e}")
    print("---")
    print("Assicurati che MongoDB sia installato e in esecuzione sulla porta 27017.")
except Exception as e:
    print(f"Si è verificato un errore generico: {e}")