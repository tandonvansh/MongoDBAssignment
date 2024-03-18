from connection import connect_to_database


def insert_documents(collection_name, documents):
    # Get the collection based on the collection name
    collection = getattr(db, collection_name)
    print(f"Attempting to insert documents into collection: {collection_name}")

    try:
        result = collection.insert_many(documents)
        print(f"Documents inserted successfully into {collection_name} with inserted IDs:", result.inserted_ids)
    except Exception as e:
        print(f"Error inserting documents into {collection_name}:", e)

    
if __name__ == "__main__":
    db = connect_to_database()
    # sample data
    sample_comments = [
    { 
        "name": "sample comment",
        "email": "harry_lloyd@gamesofthron.es",
        "movie_id":
            { 
                "$oid": "573a1394f29313caabce010b" 
            },
        "text": "this is a sample comment to check insertion.",
        "date": 
            { 
                "$date": 
                    { 
                    "$numberLong": "409674428000" 
                    }
            } 
    },
    {
        "name": "sample comment 2",
        "email": "harry_lloy@gamesofthron.es",
        "movie_id":
            { 
                "$oid": "573a1394f29313caabce010b" 
            },
        "text": "this is another sample comment to check insertion.",
        "date": 
            { 
                "$date": 
                    { 
                    "$numberLong": "409674428000" 
                    }
            } 
    }
    ]
    
    sample_movies = [
        {
        
        "plot": "At 10 years old, Owens becomes a ragged orphan when his sainted mother dies. The Conways, who are next door neighbors, take Owen in, but the constant drinking by Jim soon puts Owen on the ...",
        "genres": [
            "Biography",
            "Crime",
            "Drama"
        ],
        "runtime": {
            "$numberInt": "72"
        },
        "rated": "PASSED",
        "cast": [
            "John McCann",
            "James A. Marcus",
            "Maggie Weston",
            "Harry McCoy"
        ],
        "num_mflix_comments": {
            "$numberInt": "1"
        },
        "poster": "https://m.media-amazon.com/images/M/MV5BNDkxZGU4NmMtODJlNy00YzA2LTg4ZGMtNGFlNzAyNzcxOTM1XkEyXkFqcGdeQXVyOTM3MjcyMjI@._V1_SY1000_SX677_AL_.jpg",
        "title": "Sample movie",
        "fullplot": "At 10 years old, Owens becomes a ragged orphan when his sainted mother dies. The Conways, who are next door neighbors, take Owen in, but the constant drinking by Jim soon puts Owen on the street. By 17, Owen learns that might is right. By 25, Owen is the leader of his own gang who spend most of their time gambling and drinking. But Marie comes into the gangster area of town and everything changes for Owen as he falls for Marie. But he cannot tell her so, so he comes to her settlement to find education and inspiration. But soon, his old way of life will rise to confront him again.",
        "languages": [
            "English"
        ],
        "released": {
            "$date": {
            "$numberLong": "-1713657600000"
            }
        },
        "directors": [
            "Raoul Walsh"
        ],
        "writers": [
            "Owen Frawley Kildare (book)",
            "Raoul Walsh (adapted from the book: \"My Mamie Rose\")",
            "Carl Harbaugh (adapted from the book: \"My Mamie Rose\")"
        ],
        "awards": {
            "wins": {
            "$numberInt": "1"
            },
            "nominations": {
            "$numberInt": "0"
            },
            "text": "1 win."
        },
        "lastupdated": "2015-08-14 01:28:18.957000000",
        "year": {
            "$numberInt": "1915"
        },
        "imdb": {
            "rating": {
            "$numberDouble": "6.8"
            },
            "votes": {
            "$numberInt": "626"
            },
            "id": {
            "$numberInt": "5960"
            }
        },
        "countries": [
            "USA"
        ],
        "type": "movie",
        "tomatoes": {
            "viewer": {
            "rating": {
                "$numberDouble": "3.4"
            },
            "numReviews": {
                "$numberInt": "395"
            },
            "meter": {
                "$numberInt": "70"
            }
            },
            "dvd": {
            "$date": {
                "$numberLong": "1006819200000"
            }
            },
            "critic": {
            "rating": {
                "$numberDouble": "9.2"
            },
            "numReviews": {
                "$numberInt": "5"
            },
            "meter": {
                "$numberInt": "100"
            }
            },
            "lastUpdated": {
            "$date": {
                "$numberLong": "1442510562000"
            }
            },
            "rotten": {
            "$numberInt": "0"
            },
            "production": "Fox Film Corporation",
            "fresh": {
            "$numberInt": "5"
            }
        }
    }
    ]
    
    sample_users = [
        
        {
            "name":"Sansa clark",
            "email":"sophie_ter@gameofthron.es",
            "password":"$2b$12$nCIRE81..AtAoysPZkl19.G5V0EdIwwsZh1f18lxWEr3dlpG/Uusi"
        }
        
    ]
    
    sample_theatres = [
        {
            "theaterId":{"$numberInt":"10211"},
            "location":
                {
                    "address":
                        {
                            "street1":"sample",
                            "city":"El Seundo",
                            "state":"USA",
                            "zipcode":"90245"
                        },
                    "geo":
                        {
                            "type":"Point",
                            "coordinates":
                            [
                                    {
                                        "$numberDouble":"-128.3960826"
                                    },
                                    {
                                        "$numberDouble":"33.9051385"
                                    }
                            ]
                        }
                }   
        }
    ]

    insert_documents("comments", sample_comments)
    insert_documents("movies", sample_movies)
    insert_documents("users" , sample_users)
    insert_documents("theatres", sample_theatres)
    
    
    
    
    