from bs4 import BeautifulSoup
import requests
import urllib.request
import os
import csv
import time
import oracledb
import traceback


##### random helper methods #####

def get_time():
    return time.strftime("%Y-%m-%dT%H-%M-%SZ%z")

def get_data(url):
    r = requests.get(url)
    return r.text

def grab_table(soup):
    table = []
    for row in soup.find_all('tr'):
        lst = []
        for col in row.find_all('td'):
            lst += [col]
        table += [lst]
    table = table[3:] # remove non data rows
    table = [item[1:] for item in table] # remove icon from columns
    return table

def write_to_file(table): # currently unused
    with open(f"data_{get_time()}.csv", "w") as f:
        csv_writer = csv.writer(f, delimiter = ",")
        csv_writer.writerow(["File name", "Last Modified", "Size", "Description"])
        for row in table:
            csv_writer.writerow(row)

##### database pool class #####

DB_USER = os.environ("DB_USER") # change this to grab info from OCI vault
DB_PASS = os.environ("DB_PASS")
DB_SERVICE = "(description= (retry_count=15)(retry_delay=3)\
(address=(protocol=tcps)(port=1522)(host=adb.eu-frankfurt-1.oraclecloud.com))\
(connect_data=(service_name=g6aa31b0b04d2f7_nasadw_high.adb.oraclecloud.com))\
(security=(ssl_server_dn_match=yes)))" # you can find this string in the tnsnames.ora file. service used is "nasadw_high"
DB_SCHEMA = "STARLINK_USER"
WALLET_LOC = os.environ("WALLET_LOC") # location of the ewallet.pem file from the wallet
WALLET_PASS = os.environ("WALLET_PASS") # password for the wallet

##### SQL definitions #####
SQL_getLastTime = f"""
SELECT *
FROM (
    SELECT *
    FROM {DB_SCHEMA}.GFSC_DATA
    ORDER BY LAST_MODIFIED DESC
)
WHERE ROWNUM = 1
"""

##### main method #####

def main():
    base_url = "https://is.sci.gsfc.nasa.gov/gsfcdata/jpss1/viirs/level2/"
    qparams = "?C=S;O=A"
    print(f"{get_time()}|Retrieving data...")
    #htmldata = get_data(base_url + qparams)
    with open("/Users/mjchen/Desktop/Innovation/Accenture_Satellite_Project/page.html") as f:
        htmldata = f.read()
    soup = BeautifulSoup(htmldata, 'html.parser')

    print(f"{get_time()}|Transforming data into table...")
    table = grab_table(soup)

    print(f"{get_time()}|Initializing database connection...")
    connection = oracledb.connect(
        user = DB_USER,
        password = DB_PASS,
        dsn = DB_SERVICE,
        wallet_location = WALLET_LOC,
        wallet_password = WALLET_PASS)
    
    # get the most recently inserted item
    print(f"{get_time()}|Retrieving last insert time...")
    last_insert_time = None
    try:
        with connection.cursor() as cursor:
            resp = []
            for item in cursor.execute(SQL_getLastTime):
                resp += [item]
            print(f"Response: {resp}")
            last_insert_time = resp[0]
    except:
        print(f"{get_time()}|Something went wrong. Exception summary:\n{traceback.format_exc()}")
    finally:
        connection.close()

    # filter data by last insert time
    print(last_insert_time)

if __name__ == "__main__":
    main()
    print("Done.")
