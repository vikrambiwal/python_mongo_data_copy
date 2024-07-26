from pymongo import MongoClient

# Define source and target MongoDB connection strings
source_uri = f"mongodb://source_username:source_password@source_db_host:port/source_db?authSource=admin"
target_uri = f"mongodb://target_username:target_password@target_db_host:port/target_db?authSource=admin"


# Connect to the source and target databases
source_client = MongoClient(source_uri)
target_client = MongoClient(target_uri)

# Define the database and collection names
source_db_name = "source_db"
source_collection_name = "source_collection"
target_db_name = "target_db"
target_collection_name = "target_collection"

# Get the source and target collections
source_collection = source_client[source_db_name][source_collection_name]
target_collection = target_client[target_db_name][target_collection_name]

# Copy documents from source to target collection
batch_size = 1000
number_of_rows = 0
max_num_of_rows_to_copy = 1000000
batch = []


for document in source_collection.find():
  print("Documents copied successfully!", document)
  # target_collection.insert_one(document)
  batch.append(document)
  if len(batch) == batch_size:
        number_of_rows = number_of_rows + 1
        target_collection.insert_many(batch)
        batch = []
        
  if number_of_rows >= max_num_of_rows_to_copy/batch_size:
    break


print("Loop closed successfully!")
# Close the connections
source_client.close()
target_client.close()

print("Documents copied successfully!")
