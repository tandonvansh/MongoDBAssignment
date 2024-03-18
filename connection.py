from pymongo import MongoClient

mongo_uri = "mongodb://localhost:27017/mflix_db"

# method to connect to the database
def connect_to_database():
    try:
        client = MongoClient(mongo_uri)
        db = client.mflix_db
        print("Connected to the database")
        print(db)
        return db
    except Exception as e:
        print("Error connecting to the database: ", e)
        return None

db = connect_to_database()