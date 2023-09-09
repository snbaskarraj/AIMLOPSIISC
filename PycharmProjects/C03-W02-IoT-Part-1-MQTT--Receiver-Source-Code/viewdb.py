import pymongo
import json

#Reading the configuration file
f=open("config.json")
config = json.loads(f.read())
f.close()

#Initializing connection to the database
dbclient = pymongo.MongoClient(config["db_host"], config["db_port"])
db = dbclient[config["db_name"]]
dbt = db[config["db_collection"]]

#Querying for the messages that were published to the `devices/temp` topic
entries = dbt.find({"topic":"devices/temp"})

#Print the entries
for entry in entries:
    print(entry)