from pymongo import MongoClient

try:
    client = MongoClient("mongodb://root:example@localhost:27017/")
    db = client.testdb
    print("MongoDB connected successfully!")
except Exception as e:
    print("MongoDB connection failed:", e)
