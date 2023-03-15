from bs4 import BeautifulSoup
import requests
import urllib.request
import os
import csv
import time
import json
import oracledb
import traceback
from datetime import datetime


oracledb.init_oracle_client(lib_dir = "/Users/mjchen/instantclient_19_8")

##### some constants #####
DB_USER = os.getenv("DB_USER") # change this to grab info from OCI vault
DB_PASS = os.getenv("DB_PASS")
DB_SERVICE = "(description= (retry_count=15)(retry_delay=3)\
(address=(protocol=tcps)(port=1522)(host=adb.eu-frankfurt-1.oraclecloud.com))\
(connect_data=(service_name=g6aa31b0b04d2f7_nasadw_high.adb.oraclecloud.com))\
(security=(ssl_server_dn_match=yes)))" # you can find this string in the tnsnames.ora file. service used is "nasadw_high"
DB_SCHEMA = "STARLINK_USER"
WALLET_LOC = os.getenv("WALLET_LOC") # location of the ewallet.pem file from the wallet
WALLET_PASS = os.getenv("WALLET_PASS") # password for the wallet
LAST_MODIFIED_DATE_FORMAT = "%Y-%m-%d %H:%M"

BASE_URL = os.getenv("BASE_URL")
URI = os.getenv("URI")
QPARAMS = os.getenv("QPARAMS")


##### random helper methods #####

def get_time():
    return time.strftime("%Y-%m-%dT%H-%M-%SZ%z")

def get_data(url):
    print(f"{get_time()}|GET DATA AT {url}")
    r = requests.get(url)
    print(r)
    return r.text

def convert_size(s):
    try:
        return int(s)
    except:
        try:
            return int(s[:-1]) * {"K": 1000, "M": 1000000, "G": 1000000000}[s[-1]]
        except:
            return -1

def grab_table(soup):
    table = []
    for row in soup.find_all('tr'):
        lst = []
        for col in row.find_all('td'):
            lst += [col]
        table += [lst]
    table = table[3:] # remove non data rows
    table = [item[1:] for item in table if len(item) == 5] # remove icon from columns, and omit empty rows
    return table

def truncate_table(table, last_insert_time):
    # sort the table by date first (earliest date first)
    #sorted_table = sorted(table, key = lambda x: datetime.strptime(x[1].text.strip(), LAST_MODIFIED_DATE_FORMAT))
    sorted_table = sorted(table, key = lambda x: convert_size(x[2]))
    truncated_table, rejects = [], []
    p = 0
    for item in sorted_table:
        if p < 5:
            pass#print(item, len(item))
        d_str = item[1].text.strip()
        d_obj = datetime.strptime(d_str, LAST_MODIFIED_DATE_FORMAT)
        if d_obj > last_insert_time:
            truncated_table += [item]
        else:
            rejects += [item]
        p += 1
    return truncated_table

def prepare_table(table):
    new_table = []
    row_num = 0
    for record in table:
        try:
            new_record = {
                "FILE_NAME": record[0].find("a")["href"],
                "LAST_MODIFIED": datetime.strptime(record[1].text.strip(), LAST_MODIFIED_DATE_FORMAT),
                "FILE_SIZE": record[2].text
            }
            new_table += [new_record]
        except:
            print(f"Could not parse row {row_num}: {traceback.format_exc()}")
        row_num += 1
    return new_table

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
        print(f"{get_time()}|\tINSERT {bind_vars}")
        resp = []
        try:
            with self.connection.cursor() as cursor:
                blob = bytes(get_data(BASE_URL + URI + bind_vars["FILE_NAME"]), 'utf-8')
                bind_vars["FILE_BLOB"] = blob
                res = cursor.execute(SQL_insertRecord, bind_vars)
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


##### SQL definitions #####
SQL_getLastTime = f"""
SELECT LAST_MODIFIED
FROM (
    SELECT *
    FROM {DB_SCHEMA}.GFSC_DATA
    ORDER BY LAST_MODIFIED DESC
)
WHERE ROWNUM = 1
"""
SQL_insertRecord = f"""
INSERT INTO {DB_SCHEMA}.GFSC_DATA
(FILE_NAME, LAST_MODIFIED, FILE_SIZE, FILE_BLOB)
VALUES
(:FILE_NAME, :LAST_MODIFIED, :FILE_SIZE, :FILE_BLOB)
--(:FILE_NAME, :LAST_MODIFIED, :FILE_SIZE, to_blob(utl_raw.conv_to_raw(:FILE_BLOB)))
"""

##### main method #####

def main():
    # get HTML data
    print(f"{get_time()}|Retrieving html data from NASA...")
    htmldata = get_data(BASE_URL + URI + QPARAMS)
    # with open("/Users/mjchen/Desktop/Innovation/Accenture_Satellite_Project/page.html") as f:
    #     htmldata = f.read()
    soup = BeautifulSoup(htmldata, 'html.parser')

    # transform HTML data into a table
    print(f"{get_time()}|Transforming data into table...")
    table = grab_table(soup)

    # create db connection
    print(f"{get_time()}|Initializing database connection...")
    db_conn = Database()
    
    # get the most recently inserted item
    print(f"{get_time()}|Retrieving last insert time...")
    last_insert_time = datetime.min
    resp, success = db_conn.select(SQL_getLastTime)
    if success:
        if len(resp) == 0:
            last_insert_time = datetime.min
        else:
            last_insert_time = resp[0][0]
    else:
        pass # do something if can't retrieve last insert time? probably shouldn't proceed to process new data
        last_insert_time = datetime.max # forcibly prevents addition of new data

    # filter data by last insert time, then truncate data
    print(f"{get_time()}|Filtering data by last insert time {last_insert_time}...")
    truncated_table = truncate_table(table, last_insert_time)
    print(f"{len(table)-len(truncated_table)} records omitted of {len(table)} records")
    if len(truncated_table) == 0:
        print(f"No rows to write. Stopping program...")
        return

    # convert data to json
    print(f"{get_time()}|Preparing truncated data for data insert...")
    data = prepare_table(truncated_table)
    print(f"{get_time()}|Writing data to file...")
    # write_to_file(data)

    # insert data to database
    print(f"{get_time()}|Inserting data...")
    #resp, success = db_conn.insert_many(SQL_insertRecord, data)
    rows_inserted = 0
    for item in data[:10]:
        resp, succ = db_conn.insert_data(item)
        rows_inserted += 1 if succ else 0

    if success:
        print(f"{get_time()}|Successfully inserted {rows_inserted} rows.")
    else:
        print(f"{get_time()}|ROWS MAY NOT HAVE BEEN INSERTED")

if __name__ == "__main__":
    main()
    print("Done.")
