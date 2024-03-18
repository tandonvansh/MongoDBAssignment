import json
from os import listdir
import bson.json_util
from connection import connect_to_database
from pymongo import errors

# method to load json files into mongo collections

def bulk_load_json_files():
    db = connect_to_database()
    
    json_dir = "sample_mflix/"
    
    for filename in listdir(json_dir):
        if filename.endswith(".json"):
            collection_name = filename.split(".")[0]
            collection = db[collection_name]
            total_docs = 0
            print("Loading data from file: ", filename, " into collection: ", collection_name)
            with open(json_dir + filename , 'r') as file:
                for line in file:
                    data = json.loads(line)
                    bson_data = bson.json_util.loads(bson.json_util.dumps(data))
                    
                    # inserting in mongo collection
                    try:
                        collection.insert_one(bson_data)
                        total_docs += 1
                    except errors.BulkWriteError as bwe:
                        print(f"Error inserting document into collection '{collection_name}': {bwe.details}")
        
            print(f"Total documents inserted into {collection_name} : {total_docs}")
            print("\n")
    print("Data loading completed")

bulk_load_json_files()