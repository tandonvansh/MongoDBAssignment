from connection import connect_to_database
from datetime import datetime


# theatres collections
def top_10_cities():
    pipeline = [
        {"$group": {"_id": "$location.address.city", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    result = list(db.theaters.aggregate(pipeline))
    return result

def preprocess_coordinates(coordinates):
    return [float(coord) for coord in coordinates]

def top_10_theater_nearby(longitude, latitude):

    for theater in db.theaters.find():
        coordinates = theater["location"]["geo"]["coordinates"]
        processed_coordinates = preprocess_coordinates(coordinates)
        theater["location"]["geo"]["coordinates"] = processed_coordinates
        db.theaters.update_one(
            {"_id": theater["_id"]},
            {"$set": {"location.geo.coordinates": processed_coordinates}}
        )
    
    # Create 2dsphere index on the 'location.geo.coordinates' field  
    db.theaters.create_index([("location.geo.coordinates", "2dsphere")])
    
    # Define the query to find theaters near the given coordinates
    query = {
        "location.geo.coordinates": {
            "$nearSphere": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [longitude, latitude]
                }
            }
        }
    }

    result = db.theaters.find(query).limit(10)

    return list(result)

if __name__ == "__main__":
    db = connect_to_database()
    
    top_cities = top_10_cities()
    
    print("Top 10 cities with maximum number of theaters:")
    print("\n")
    for index, city_data in enumerate(top_cities, start=1):
        city_name = city_data['_id']
        theater_count = city_data['count']
        print(f"{index}. {city_name}: {theater_count} theaters")
    
    print("\n")
        
    top_nearby_theater=top_10_theater_nearby(20,40)
    
    print("Top 10 theaters nearby the given coordinates:")
    print("\n")
    for index, theater in enumerate(top_nearby_theater, start=1):
        print(f"{index}. {theater['theaterId']} - {theater['location']['address']['street1']},{theater['location']['address']['city']}, {theater['location']['address']['state']}")