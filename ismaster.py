import pprint
import pymongo

if __name__ == '__main__':

    client = pymongo.MongoClient()
    database = client["test"]
    response = database.command("ismaster")
    pprint.pprint(response)
