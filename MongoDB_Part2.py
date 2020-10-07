# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import json
import codecs
import sys
import pymongo

# ------------------------------------------
# FUNCTION 1: most_popular_cuisine
# ------------------------------------------

#FUNCTION 1: most_popular_cuisine
# ------------------------------------------
def most_popular_cuisine(my_collection):
    #count total number of rows
    total_count = my_collection.count()
    #define an empty list
    popular_cuisine=[]
    for value in my_collection.aggregate([
        {"$group": {"_id": "$cuisine","count": { "$sum": 1 }}},{"$sort":{"count":-1}},
        {"$project": {"percentage": {"$multiply": [{"$divide": [100,total_count ]}, "$count"]}}}
    ]):
        popular_cuisine.append(value)
    return(popular_cuisine[0]["_id"],popular_cuisine[0]["percentage"])

# ------------------------------------------
# FUNCTION 2: ratio_per_borough_and_cuisine
def ratio_per_borough_and_cuisine(my_collection, cuisine):
    my_list_1=[]
    my_list_2=[]
    empty_dict = {}
    for cuisine_count in my_collection.aggregate([
        {"$match":{"cuisine":{"$eq":cuisine}}},
        {"$group":{"_id":"$borough","_count":{"$sum":1}}}
    ]):
        my_list_1 = list( my_collection.aggregate([
        {"$match":{"cuisine":{"$eq":cuisine}}},
        {"$group":{"_id":"$borough","_count":{"$sum":1}}}
    ]))

    for borough_count in my_collection.aggregate([
        {"$group": {"_id": "$borough", "count": {"$sum": 1}}}
    ]):
        my_list_2 = list(my_collection.aggregate([
        {"$group": {"_id": "$borough", "count": {"$sum": 1}}}
    ]))

    for my_key_1 in my_list_1:
        for my_key_2 in my_list_2:
            if my_key_1["_id"] == my_key_2["_id"] :
                empty_dict[my_key_2["_id"]] = (my_key_1["_count"]/my_key_2["count"])*100

    key_value_store = [[k, v] for k, v in empty_dict.items()]

    def sorybyvalue(val):
        return val[1]

    key_value_store.sort(key=lambda key_value_store:key_value_store[1])

    return(key_value_store[0][0],key_value_store[0][1])



# ------------------------------------------
# FUNCTION 3: ratio_per_zipcode
# ------------------------------------------
def ratio_per_zipcode(my_collection, cuisine, borough):
    my_list_1 = []
    my_list_2 = []
    empty_dict = {}
    for data_address in my_collection.aggregate([
        {"$match": {"borough": {"$eq": borough}}},
        {"$unwind": "$address"},
        {"$group": {"_id": "$address.zipcode", "totalcount": {"$sum": 1}}},{"$sort":{"totalcount":-1}},{"$limit":5}
        ]):
        my_list_1 = list(my_collection.aggregate([
        {"$match": {"borough": {"$eq": borough}}},
        {"$unwind": "$address"},
        {"$group": {"_id": "$address.zipcode", "totalcount": {"$sum": 1}}},{"$sort":{"totalcount":-1}},{"$limit":5}
        ]))

    for zipcode in my_collection.aggregate([
        {"$match": {"borough": {"$eq": borough}}},
        {"$match": {"cuisine": {"$eq": cuisine}}},
        {"$unwind": "$address"},
        {"$group": {"_id": "$address.zipcode", "count": {"$sum": 1}}}
    ]):
        my_list_2 = list(my_collection.aggregate([
        {"$match": {"borough": {"$eq": borough}}},
        {"$match": {"cuisine": {"$eq": cuisine}}},
        {"$unwind": "$address"},
        {"$group": {"_id": "$address.zipcode", "count": {"$sum": 1}}}
    ]))

    for my_key_1 in my_list_1:
        for my_key_2 in my_list_2:
            if my_key_1["_id"] in my_key_2["_id"]:
                empty_dict[my_key_2["_id"]] = (my_key_2["count"] / my_key_1["totalcount"]) * 100


    key_value_store = [[k, v] for k, v in empty_dict.items()]

    def sortbyvalue(val):
        return val[1]

    key_value_store.sort(key=lambda key_value_store: key_value_store[1])
    return (key_value_store[0][0], key_value_store[0][1])
# ------------------------------------------
# FUNCTION 4: best_restaurants
# ------------------------------------------
def best_restaurants(my_collection, cuisine, borough, zipcode):
    best_restro=[]
    reviews=[]
    my_list = []
    for value in my_collection.aggregate([
        {"$match": {"borough": {"$eq": borough}}},
        {"$match": {"cuisine": {"$eq": cuisine}}},
        {"$match": {"address.zipcode": {"$eq": zipcode}}},
        {"$project": {"_id": "$name", "numOfreview": {"$size": "$grades.score"},"Average":{"$avg":"$grades.score"}}},
        {"$match": {"numOfreview": {"$gte":4}}},
        {"$sort": {"Average": -1}},
        {"$limit":3}
    ]):
        my_list.append(value)

    for j in range(len(my_list)):
        best_restro.append(my_list[j]['_id'])
        reviews.append(my_list[j]['Average'])
    return(best_restro,reviews)


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------/
def my_main(database_name, collection_name):
    # 1. We set up the connection to mongos.exe allowing us to access to the cluster
    mongo_client = pymongo.MongoClient()

    # 2. We access to the desired database
    db = mongo_client.get_database(database_name)

    # 3. We access to the desired collection
    collection = db.get_collection(collection_name)

    # 4. What is the kind of cuisine with more restaurants in the city?
    (cuisine, ratio_cuisine) = most_popular_cuisine(collection)
    print("1. The kind of cuisine with more restaurants in the city is", cuisine, "(with a", ratio_cuisine, "percentage of restaurants of the city)")

    # 5. Which is the borough with smaller ratio of restaurants of this kind of cuisine?
    (borough, ratio_borough) = ratio_per_borough_and_cuisine(collection, cuisine)
    print("2. The borough with smaller ratio of restaurants of this kind of cuisine is", borough, "(with a", ratio_borough, "percentage of restaurants of this kind)")

    # 6. Which of the 5 biggest zipcodes of the borough has a smaller ratio of restaurants of the cuisine we are looking for?
    (zipcode, ratio_zipcode) = ratio_per_zipcode(collection, cuisine, borough)
    print("3. The zipcode of the borough with smaller ratio of restaurants of this kind of cuisine is zipcode =", zipcode, "(with a", ratio_zipcode, "percentage of restaurants of this kind)")

    # 7. Which are the best 3 restaurants (of the kind of cuisine we are looking for) of our zipcode?
    (best, reviews) = best_restaurants(collection, cuisine, borough, zipcode)
    print("4. The best three restaurants (of this kind of couisine) at these zipcode are:", best[0], "(with average reviews score of", reviews[0], "),", best[1], "(with average reviews score of", reviews[1], "),", best[2], "(with average reviews score of", reviews[2], ")")

    # 8. Close the client
    mongo_client.close()

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We get the input arguments
    my_database = "test"
    my_collection = "restaurants"

    # 2. We call to my_main
    my_main(my_database, my_collection)
