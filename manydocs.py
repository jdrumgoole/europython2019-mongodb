import pymongo
import string
from datetime import datetime
import random


def random_string(size, letters=string.ascii_letters):
    return "".join([random.choice(letters) for _ in range(size)])


def make_article(username, count):
    return {"_id": f"article{count}",
            "title": random_string(20),
            "body": random_string(20),
            "author": username,
            "postdate": datetime.utcnow()}


def make_user(count):
    return {"username": f"user{count}",
            "password": random_string(10),
            "karma": str(random.randint(0, 500)),
            "lang": "EN"}


client = pymongo.MongoClient()
ep2019 = client["EP2019STRDEMO"]

ep2019.drop_collection("users")
ep2019.drop_collection("articles")

users_collection = ep2019["users"]
articles_collection = ep2019["articles"]

users = []
articles = []
for i in range(1000000):

    user = make_user(i)
    users.append(user)
    articles.append(make_article(user["username"], i))
    if i % 500 == 0:
        users_collection.insert_many(users)
        articles_collection.insert_many(articles)
        users = []
        articles = []
        print(f"Inserted {i} users")
        print(f"Inserted {i} articles")

if articles:
    articles_collection.insert_many(articles)
    print(f"Inserted {i} articles")

if users:
    users_collection.insert_many(users)
    print(f"Inserted {i} users")
