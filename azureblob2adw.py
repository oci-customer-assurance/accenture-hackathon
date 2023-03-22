import os
from azure.storage.blob import BlobServiceClient
import azure.core.exceptions
from dotenv import load_dotenv
import time
from bs4 import BeautifulSoup
import requests
import urllib.request
import csv
import json
import oracledb
import traceback
from datetime import datetime

load_dotenv()

##### aspose install #####
convert_tiff_to_png = False
doc, builder = None, None
try:
    import aspose.words as aw
    doc = aw.Document()
    builder = aw.DocumentBuilder(doc)
    convert_tiff_to_png = True
except:
    print("Could not import aspose_words. Cannot convert .tif files to .png files.")


##### constants #####

print("Loading constants...")

DB_USER = os.getenv("DB_USER") # change this to grab info from OCI vault
DB_PASS = os.getenv("DB_PASS")
DB_SERVICE = os.getenv("DB_SERVICE") # you can find this string in the tnsnames.ora file. service used is "nasadw_high"
DB_SCHEMA = "STARLINK_USER"
WALLET_LOC = os.getenv("WALLET_LOC") # location of the ewallet.pem file from the wallet
WALLET_PASS = os.getenv("WALLET_PASS") # password for the wallet
LAST_MODIFIED_DATE_FORMAT = "%Y-%m-%d %H:%M"

CONN_STR = os.getenv("az_storage_conn_str")
CONTAINER_NAME = os.getenv("container_name")
BLOB_NAME = os.getenv("blob_name")
FILE_LOC = os.getenv("file_loc")


##### helpers #####

def get_time():
    return time.strftime("%Y-%m-%dT%H-%M-%SZ%z")

def convert_size(s):
    try:
        return int(s)
    except:
        try:
            return int(s[:-1]) * {"K": 1000, "M": 1000000, "G": 1000000000}[s[-1]]
        except:
            return -1

def write_to_file(table): # currently unused
    with open(f"data_{get_time()}.csv", "w") as f:
        # csv_writer = csv.writer(f, delimiter = ",")
        # csv_writer.writerow(["FILE NAME", "LAST MODIFIED", "FILE SIZE", "FILE BLOB"])
        # for row in table:
        #     csv_writer.writerow(row.values())
        f.write(json.dumps(table, indent=4))


##### database class #####

class Database:
    def __init__(self):
        self.connection = oracledb.connect(
            user = DB_USER,
            password = DB_PASS,
            dsn = DB_SERVICE,
            wallet_location = WALLET_LOC,
            wallet_password = WALLET_PASS)

    def insert(self, sql, bind_vars = None):
        resp = []
        try:
            with self.connection.cursor() as cursor:
                if bind_vars is None:
                    res = cursor.execute(sql)
                else:
                    res = cursor.execute(sql, bind_vars)
                self.connection.commit()
                if res is None:
                    return None, True
                for item in res:
                    resp += [item]
                print(f"Response: {resp}") # for debug purposes
        except:
            print(f"{get_time()}|Something went wrong. Exception summary:\n{traceback.format_exc()}")
            return None, False
        return resp, True

    def select(self, sql, bind_vars = None):
        resp = []
        try:
            with self.connection.cursor() as cursor:
                if bind_vars is None:
                    res = cursor.execute(sql)
                else:
                    res = cursor.execute(sql, bind_vars)
                if res is None:
                    return None, True
                for item in res:
                    resp += [item]
                print(f"Response: {resp}") # for debug purposes
        except:
            print(f"{get_time()}|Something went wrong. Exception summary:\n{traceback.format_exc()}")
            return None, False
        return resp, True

    def insert_many(self, sql, bind_vars):
        resp = []
        try:
            with self.connection.cursor() as cursor:
                res = cursor.executemany(sql, bind_vars)
                self.connection.commit()
                if res is None:
                    return None, True
                for item in res:
                    resp += [item]
                print(f"Response: {resp}") # for debug purposes
        except:
            print(f"{get_time()}|Something went wrong. Exception summary:\n{traceback.format_exc()}")
            return None, False
        return resp, True

    def insert_data(self, bind_vars):
        #print(f"{get_time()}|\tINSERT {bind_vars}")
        resp = []
        try:
            with self.connection.cursor() as cursor:
                res = cursor.execute(SQL_insertRecord, bind_vars)
                self.connection.commit()
                if res is None:
                    return None, True
                for item in res:
                    resp += [item]
                #print(f"Response: {str(resp)[:100]}") # for debug purposes
        except:
            print(f"{get_time()}|Something went wrong. Exception summary:\n{traceback.format_exc()}")
            return None, False
        return resp, True


##### AzureBlob class #####
class AzureBlob:
    def __init__(self, conn_str):
        self.blob_service_client = BlobServiceClient.from_connection_string(conn_str)
        self.container_client = None
        self.container_name = None
        self.blob_client = None
        self.blob_name = None

    def init_container(self, container_name):
        try:
            self.container_client = self.blob_service_client.create_container(container_name)
            print(f"\tAzureBlob.init_container: Container {container_name} created.")
        except azure.core.exceptions.ResourceExistsError:
            self.container_client = self.blob_service_client.get_container_client(container_name)
            print(f"\tAzureBlob.init_container: Container {container_name} already exists. Grabbing existing container...")
        except:
            print(f"\tAzureBlob.init_container: Container {container_name} could not be retrieved.")
            return False
        self.container_name = container_name
        return True

    def get_blob_client(self, blob_name):
        if self.container_name is None:
            print(f"\tAzureBlob.get_blob_client: container_name is null (need to initialize container first)")
            return False
        self.blob_client = self.blob_service_client.get_blob_client(
            container = self.container_name,
            blob = blob_name)
        self.blob_name = blob_name
        return True

    def write_to_blob(self, bdata = b""):
        if type(bdata) is not bytes:
            try:
                bdata = bytes(bdata)
            except:
                print(f"\tAzureBlob.write_to_blob: data cannot be cast to bytes. First 100 characters: {str(bdata)[:100]}")
                return
        if self.blob_client is None:
            print(f"\tAzureBlob.write_to_blob: blob_client has not been initialized")
        blob_client_upload_blob_response = self.blob_client.upload_blob(bdata)
        print(f"\tAzureBlob.write_to_blob: wrote {len(bdata)} bytes to {self.blob_name}")

    def download_blob(self, blob_name):
        try:
            download_blob_response = self.container_client.download_blob(blob_name).readall()
            print(f"\tAzureBlob.download_blob: downloaded {blob_name} at {len(download_blob_response)} bytes")
            return download_blob_response
        except:
            print(f"\tAzureBlob.download_blob: could not download blob {blob_name}. Details:\n")
            print(traceback.format_exc())
            print()
            return None

    def list_blobs(self):
        if self.container_client is None:
            print("\tAzureBlob.list_blobs: container_client has not been initialized")
            return []
        list_blobs_response = self.container_client.list_blobs() # this returns an iterator
        blob_list = []
        for blob in list_blobs_response:
            blob_list += [blob]
            print(blob.name)
        return blob_list


##### SQL definitions #####

SQL_insertRecord = f"""
INSERT INTO {DB_SCHEMA}.IMAGE_DATA
(FILE_NAME, FILE_SIZE, FILE_BLOB)
VALUES
(:FILE_NAME, :FILE_SIZE, :FILE_BLOB)
"""


##### main method #####

def main():
    # get data from Azure
    ablob = AzureBlob(CONN_STR)
    ablob.init_container(CONTAINER_NAME)
    ablob.get_blob_client(f"{get_time()}_{BLOB_NAME}")
    bfile = b""
    with open(FILE_LOC, "rb") as f:
        bfile = f.read()
    ablob.write_to_blob(bfile)
    blob_list = ablob.list_blobs()
    print(blob_list)

    # create db connection
    print(f"{get_time()}|Initializing database connection...")
    db_conn = Database()

    # generate blob data
    data = []
    for blob in blob_list:
        data += [{
            "FILE_NAME": blob.name,
            "FILE_SIZE": blob.size
        }]

    # insert blobs into database
    print(f"{get_time()}|Inserting data...")
    rows_inserted = 0
    for item in data:
        item["FILE_BLOB"] = ablob.download_blob(item["FILE_NAME"])
        if convert_tiff_to_png and item["FILE_NAME"].split(".")[-1].lower()[:3] == "tif":
            try:
                tif_file = "data/" + item["FILE_NAME"]
                png_file = ".".join(tif_file.split(".")[:-1] + ["png"])
                with open(tif_file, "wb") as f:
                    f.write(item["FILE_BLOB"])
                shape = builder.insert_image(tif_file)
                shape.image_data.save(png_file)
                with open(png_file, "rb") as f:
                    item["FILE_BLOB"] = f.read()
            except:
                print(f"Could not convert {item["FILE_NAME"]} to PNG. Inserting as is.")
                print(traceback.format_exc())
        resp, succ = db_conn.insert_data(item)
        rows_inserted += 1 if succ else 0

    print(f"{get_time()}|Successfully inserted {rows_inserted} rows.")

if __name__ == "__main__":
    main()
    print("Done.")
