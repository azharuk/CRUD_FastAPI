from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta
from bson import ObjectId
import time
import threading

#from fastapi.staticfiles import StaticFiles
#from fastapi.responses import RedirectResponse

# MongoDB Connection
client = MongoClient("mongodb://localhost:27017/")
db = client["courses_db"]
collection = db["courses"]

# Set expiration time
expire_time = timedelta(minutes=10)

# Event to signal thread to stop
stop_event = threading.Event()

def normalize_and_store(dataframe):
    normalized_data = dataframe.to_dict(orient='records')
    current_time = datetime.utcnow()
    for record in normalized_data:
        record["inserted_at"] = current_time
    collection.insert_many(normalized_data)
    print(f"Inserted {len(normalized_data)} records into the collection.")

# Function to download and normalize data
def fetch_and_normalize_data():
    url = "https://api.mockaroo.com/api/501b2790?count=100&key=8683a1c0"
    df = pd.read_csv(url)
    normalize_and_store(df)

# Check for expiration
def check_expiration():
    while not stop_event.is_set():
        current_time = datetime.utcnow()
        expired_count = collection.count_documents({"inserted_at": {"$lt": current_time - expire_time}})
        total_documents = collection.count_documents({})

        if expired_count > 0 or total_documents == 0:
            print("Old data found or collection is empty. Dropping the collection and reloading data.")
            collection.drop()  # Dropping the collection if any documents are expired or if the collection is empty

            # Create a TTL index on the "inserted_at" field with a 20-minute expiration, it will remove by Mongo only if the expiration check thread fail doing that
            collection.create_index("inserted_at", expireAfterSeconds=1200)

            fetch_and_normalize_data()  # Re-fetch and normalize data
        else:
            print(f"Data is up-to-date. {total_documents} records are present.")

        # Sleep for 10 minutes and 6 seconds  before checking again
        stop_event.wait(606)

def start_expiration_check():
    # Start the expiration check thread
    expiration_thread = threading.Thread(target=check_expiration, daemon=True)
    expiration_thread.start()
    return expiration_thread

def stop_expiration_check():
    # Signal the thread to stop
    stop_event.set()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the background expiration check task
    start_expiration_check()
    print(" lifespan Starting up")
    yield
    # Perform shutdown tasks here
    stop_expiration_check()
    print("Shutting down")

app = FastAPI(lifespan=lifespan)

# Define the origins that should be allowed
#origins = [
#    "https://app.wpgsoft.com",  # prod Angular app's URL
#    "http://localhost:4200",  # dev Angular app's URL
#    "http://localhost:8000",  # FastAPI static served Angular app's URL
#]

#app.add_middleware(
#    CORSMiddleware,
#    allow_origins=origins,  # Allows requests from specified origins
#    allow_credentials=True,
#    allow_methods=["*"],  # Allows all HTTP methods
#    allow_headers=["*"],  # Allows all HTTP headers
#)

@app.get("/get_courses/")
async def get_courses(search: str = "", page: int = 1, page_size: int = 10):
    query = {"$or": [
        {"University": {"$regex": search, "$options": "i"}},
        {"City": {"$regex": search, "$options": "i"}},
        {"Country": {"$regex": search, "$options": "i"}},
        {"CourseName": {"$regex": search, "$options": "i"}},
        {"CourseDescription": {"$regex": search, "$options": "i"}}
    ]}
    total = collection.count_documents(query)
    courses = list(collection.find(query).skip((page - 1) * page_size).limit(page_size))

    # Convert ObjectId to string
    for course in courses:
        course["_id"] = str(course["_id"])

    return {"total": total, "courses": courses}

@app.post("/create_course/")
async def create_course(course: dict):
    course["inserted_at"] = datetime.utcnow()  # Add the inserted_at timestamp
    result = collection.insert_one(course)
    return {"id": str(result.inserted_id)}

@app.put("/update_course/{course_id}")
async def update_course(course_id: str, course: dict):
    result = collection.update_one({"_id": ObjectId(course_id)}, {"$set": course})
    if result.matched_count:
        return {"status": "success"}
    raise HTTPException(status_code=404, detail="Course not found")

@app.delete("/delete_course/{course_id}")
async def delete_course(course_id: str):
    result = collection.delete_one({"_id": ObjectId(course_id)})
    if result.deleted_count:
        return {"status": "success"}
    raise HTTPException(status_code=404, detail="Course not found")




if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)