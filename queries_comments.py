from connection import connect_to_database
from pymongo.read_concern import ReadConcern
from datetime import datetime


# 1. Comments collection
def top_users_with_most_comments(db):
    pipeline = [
        {
            "$sortByCount": "$name"
        },
        {
            "$limit": 10
        }
    ]
    result = db.comments.aggregate(pipeline)
    return list(result)

def top_movies_with_most_comments(db):
    pipeline = [
        {"$group": {"_id": "$movie_id", "total_comments": {"$sum": 1}}},
        {"$lookup": {"from": "movies", "localField": "_id", "foreignField": "_id", "as": "movie"}},
        {"$unwind": "$movie"},
        {"$project": {"title": "$movie.title", "total_comments": 1}},
        {"$sort": {"total_comments": -1}},
        {"$limit": 10}
    ]
    result = db.comments.aggregate(pipeline)
    return list(result)

def comments_per_month_in_year(db, year):
    pipeline = [
        {"$match": {"date": {"$gte": datetime(year, 1, 1), "$lt": datetime(year + 1, 1, 1)}}},
        {"$group": {"_id": {"$month": "$date"}, "total_comments": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    result = db.comments.aggregate(pipeline)
    return list(result)

if __name__ == "__main__":
    db = connect_to_database()


    top_users  = top_users_with_most_comments(db)
    print("Top 10 users with most comments: " + "\n")
    
    for doc in top_users:
        print(f"{doc['_id']} : {doc['count']} comments")
    print("\n")
    
    top_movies = top_movies_with_most_comments(db)
    print("Top 10 movies with most comments: " + "\n")
    for doc in top_movies:
        print(f"{doc['title']} : {doc['total_comments']} comments")
    
    print("\n")
    
    year = 1998
    comments_per_month = comments_per_month_in_year(db, year)
    print(f"Comments per month in {year}: " + "\n")
    if len(comments_per_month) == 0:
        print(f"No comments found for the year {year}")
    else:
        for doc in comments_per_month:
            print(f"{doc['_id']} : {doc['total_comments']} comments")
        
    print("\n")
    
    
    
    
    
    
        
  

    
    
    
    
    
    
    
    

    
    