import os
from azure.storage.blob import BlobServiceClient
import azure.core.exceptions
from dotenv import load_dotenv
import time

load_dotenv()
conn_str = os.getenv("az_storage_conn_str")
container_name = os.getenv("container_name")
blob_name = os.getenv("blob_name")
file_loc = os.getenv("file_loc")

##### helpers #####

def get_time():
    return time.strftime("%Y-%m-%dT%H-%M-%SZ%z")

##### main method stuff #####

blob_name = f"{get_time()}_{blob_name}"

# Initialize a BlobServiceClient object
print("Initializing BlobServiceClient")
blob_service_client = BlobServiceClient.from_connection_string(conn_str)

# Create a container for the blob
print("Creating container")
try:
	container_client = blob_service_client.create_container(container_name)
except azure.core.exceptions.ResourceExistsError:
	print("\tContainer already exists. Grabbing existing container...")
	container_client = blob_service_client.get_container_client(container_name)

# Initialize a blob client
print("Initializing blob client")
blob_client = blob_service_client.get_blob_client(
	container = container_name,
	blob = blob_name)

# Write a file to the blob client
with open(file_loc, "rb") as f:
	blob_client_upload_blob_response = blob_client.upload_blob(f)

# Print the blob names in the container
print("Listing blobs:")
blob_list = container_client.list_blobs()
for blob in blob_list:
	print(f"\t{blob.name}")

# Download a blob to local
with open("/Users/mjchen/Desktop/BURRITO.png", "wb") as f:
	f.write(container_client.download_blob(blob_name).readall())

print("Done.")