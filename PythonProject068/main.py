from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Get database credentials from environment variables
username = os.getenv("MONGO_INITDB_ROOT_USERNAME")
password = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
host = os.getenv("MONGO_HOST")
port = os.getenv("MONGO_PORT")
db_name = os.getenv("MONGO_DB")

# Construct the connection URI and connect to MongoDB
uri = f"mongodb://{username}:{password}@{host}:{port}/"
client = MongoClient(uri)
db = client[db_name]
collection = db["users"]

# --- CRUD Operations ---

# CREATE a new user
def create_user(name, email):
    user_document = {"name": name, "email": email}
    result = collection.insert_one(user_document)
    print(f"User created with ID: {result.inserted_id}")

# READ all users
def read_users():
    users = collection.find()
    for user in users:
        print(user)

# UPDATE a user's email
def update_user(name, new_email):
    query = {"name": name}
    new_values = {"$set": {"email": new_email}}
    result = collection.update_one(query, new_values)
    print(f"Documents matched: {result.matched_count}, Documents modified: {result.modified_count}")

# DELETE a user
def delete_user(name):
    query = {"name": name}
    result = collection.delete_one(query)
    print(f"Documents deleted: {result.deleted_count}")

# --- Test the functions ---
if __name__ == "__main__":
    print("--- Connecting to MongoDB! ---")

    # Clean up previous runs
    collection.delete_many({})
    print("Cleaned up existing users.")

    # Run tests
    create_user("Arham", "arham@example.com")
    create_user("Asad", "asad@example.com")

    print("\nAll users:")
    read_users()

    print("\nUpdating Asad's email...")
    update_user("Asad", "asadahmad@newmail.com")

    print("\nUsers after update:")
    read_users()

    print("\nDeleting Arham...")
    delete_user("Arham")

    print("\nFinal list of users:")
    read_users()