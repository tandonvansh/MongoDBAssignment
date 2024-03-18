from connection import connect_to_database
from datetime import datetime


# 2. Movies collection
def top_movies_highest_imdb_rating(db, N):
    pipeline = [
        {"$sort": {"imdb.rating": -1}},
        {"$limit": N},
        {"$project": {"title": 1}}
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)

def top_movies_highest_imdb_rating_in_year(db, N, year):
    pipeline = [
        {"$match": {"year": year}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": N},
        {"$project": {"_id": 1, "title": 1}}
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)


def top_movies_by_imdb_rating_with_votes(db, N):
    result = db.movies.find({"imdb.votes": {"$gt": 1000}}).sort("imdb.rating", -1).limit(N)
    return list(result)

def top_movies_by_title_pattern_sorted_by_tomatoes(db, N, pattern):
    result = db.movies.find({"title": {"$regex": pattern, "$options": "i"}}).sort("tomatoes.viewer.rating", -1).limit(N)
    return list(result)


def top_directors_by_movie_count(db, N):
    pipeline = [
        {"$unwind": "$directors"},
        {"$group": {"_id": "$directors", "total_movies": {"$sum": 1}}},
        {"$sort": {"total_movies": -1}},
        {"$limit": N}
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)

def top_directors_by_movie_count_in_year(db, N, year):
    pipeline = [
        {"$match": {"year": year}},
        {"$unwind": "$directors"},
        {"$group": {"_id": "$directors", "total_movies": {"$sum": 1}}},
        {"$sort": {"total_movies": -1}},
        {"$limit": N}
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)

def top_directors_by_genre(db, N, genre):
    pipeline = [
        {"$unwind": "$directors"},
        {"$match": {"genres": genre}},
        {"$group": {"_id": "$directors", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": N}
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)


def top_actors_by_movie_count(db, N):
    pipeline = [
        {"$unwind": "$cast"},
        {"$group": {"_id": "$cast", "total_movies": {"$sum": 1}}},
        {"$sort": {"total_movies": -1}},
        {"$limit": N}
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)

def top_actors_by_movie_count_in_year(db, N, year):
    pipeline = [
        {"$match": {"year": year}},
        {"$unwind": "$cast"},
        {"$group": {"_id": "$cast", "total_movies": {"$sum": 1}}},
        {"$sort": {"total_movies": -1}},
        {"$limit": N}
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)

def top_actors_by_genre(db, N, genre):
    pipeline = [
        {"$unwind": "$cast"},
        {"$match": {"genres": genre}},
        {"$group": {"_id": "$cast", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": N}
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)


def top_movies_by_genre_with_highest_imdb_rating(db, N):
    pipeline = [
        {"$unwind": "$genres"},
        {"$group": {"_id": "$genres", "top_movies": {"$push": "$$ROOT"}}},
        {"$project": {"top_movies": {"$slice": ["$top_movies", N]}}},
        {"$unwind": "$top_movies"},
        {"$sort": {"_id": 1, "top_movies.imdb.rating": -1}},  # Sort by genre and then by IMDb rating
        {"$replaceRoot": {"newRoot": "$top_movies"}}
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)


if __name__ == "__main__":
    db = connect_to_database()
    
    print("Top 5 movies by imdb rating: ")
    for doc in top_movies_highest_imdb_rating(db, 5):
        print(doc['title'])
    print("\n")
    
    year = 1998
    print("Top 5 movies by imdb rating in year: " + str(year) + "\n")
    for doc in top_movies_highest_imdb_rating_in_year(db, 5, year):
        print(f"{doc['title']}")
    print("\n")
    
    print("Top 5 movies by imdb rating with votes greater than 1000: (title , rating , votes)" + "\n")
    for doc in top_movies_by_imdb_rating_with_votes(db, 5):
        print(f"{doc['title']} : {doc['imdb']['rating']} : {doc['imdb']['votes']}")
    print("\n")
    
    pattern = "matrix"
    print(f"Top 5 movies with title pattern '{pattern}' sorted by tomatoes viewer rating: " + "\n")
    
    for doc in top_movies_by_title_pattern_sorted_by_tomatoes(db, 5, pattern):
        print(f"{doc['title']} : {doc['tomatoes']['viewer']['rating']}")
    print("\n")
    
    print("Top 5 directors by movie count: " + "\n")
    for doc in top_directors_by_movie_count(db, 5):
        print(f"{doc['_id']} : {doc['total_movies']}")
        
    print("\n")
    
    year = 2003
    print(f"Top 5 directors by movie count in year {year}: " + "\n")
    for doc in top_directors_by_movie_count_in_year(db, 5, year):
        print(f"{doc['_id']} : {doc['total_movies']}")
    print("\n")
    
    genre = "Action"
    print(f"Top 5 directors by movie count in genre {genre}: " + "\n")
    for doc in top_directors_by_genre(db, 5, genre):
        print(f"{doc['_id']} : {doc['count']}")
    print("\n")
    
    print("Top 5 actors by movie count: " + "\n")
    for doc in top_actors_by_movie_count(db, 5):
        print(f"{doc['_id']} : {doc['total_movies']}")
    print("\n")
    
    print("Top 5 actors by movie count in year 2003: " + "\n")
    for doc in top_actors_by_movie_count_in_year(db, 5, 2003):
        print(f"{doc['_id']} : {doc['total_movies']}")
    
    print("\n")
    
    print("Top 5 actors by movie count in genre Action: " + "\n")
    for doc in top_actors_by_genre(db, 5, "Action"):
        print(f"{doc['_id']} : {doc['count']}")
    print("\n")
    
    print("Top 5 movies by genre with highest imdb rating: " + "\n")
    for doc in top_movies_by_genre_with_highest_imdb_rating(db, 5):
        print(f"{doc['title']} : {doc['genres']} : {doc['imdb']['rating']}")
    print("\n")