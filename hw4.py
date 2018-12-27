from pymongo import MongoClient # Install for using this lib.
import subprocess
from  user_definition import *

def import_query(dbname, collection_name, input_file_name):
    mongoimport_query = "mongoimport --db %s --collection %s --file %s" % (dbname, collection_name, input_file_name)#COMPLETE THIS.
    return mongoimport_query
 
#Create connection 
client = MongoClient() #default-localhost:27017
#Connect to database
db = client[dbname]

#Drop table.
db[collection_name].drop()

#Insert all data from the input_file_name.
#(To be easy, let's load the entire .json file..)
#Q1.
mongoimport_query = import_query(dbname, collection_name, input_file_name)
subprocess.call(mongoimport_query,shell=True)

# Q2.
#Find all documents which "borrower" include borrower_name in user-definition and update "borrower" to borrower_name
#Ex. REPUBLIC OF KENYA ==> KENYA
#COMPLETE THIS.
db[collection_name].update_many({"borrower":{"$regex":borrower_name}}, {"$set":{"borrower":borrower_name}})

# Q3. 
#Find documents include "borrower" which is same as borrower_name and print its borrower name, url in ascending order.
#
#Field suppressor : https://docs.mongodb.com/manual/tutorial/project-fields-from-query-results/
#Sort : https://docs.mongodb.com/manual/reference/method/cursor.sort/
for doc in db[collection_name].find({"borrower":borrower_name}, {"_id":0, "url":1, "borrower":1})\
        .sort("url"):#COMPLETE THIS.
   print doc
