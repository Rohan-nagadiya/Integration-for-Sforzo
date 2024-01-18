import pymongo
import json


db = "store_data"
table = "patient_data"
conn = pymongo.MongoClient("mongodb+srv://rohannagadiya:sPQQNqNbp2vE3A3Z@database.56qutkr.mongodb.net/")
mydb = conn[db]
conn1 = mydb[table]


query = {}
cursor = conn1.find(query)

json_data = []


for document in cursor:
    document.pop('_id', None)
    json_data.append(document)

conn.close()

with open("data.json", "w") as json_file:
    json.dump(json_data, json_file, indent=4)

print("JSON data (excluding _id field) has been saved to data.json")
