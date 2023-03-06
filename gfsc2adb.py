from bs4 import BeautifulSoup
import requests
import urllib.request
import os
import csv
import time
import cx_Oracle

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
DB = "nasadw_high"
DB_SCHEMA = "STARLINK_USER"

class DatabasePool:
    def __init__(self):
        """Constructor method to create the database session pool
        """
        self.pool = cx_Oracle.SessionPool(
            user=DB_USER,
            password=DB_PASS,
            dsn=DB,
            min=15,
            max=15,
            increment=0,
            encoding="UTF-8",
            threaded=True,
            getmode=cx_Oracle.SPOOL_ATTRVAL_WAIT)

    def get(self):
        """Getter method that returns the pool

        Returns:
            cx_Oracle.SessionPool: database pool
        """
        return self.pool

##### main method #####

def main():
    base_url = "https://is.sci.gsfc.nasa.gov/gsfcdata/jpss1/viirs/level2/"
    qparams = "?C=S;O=A"
    htmldata = get_data(base_url + qparams)
    soup = BeautifulSoup(htmldata, 'html.parser')

    table = grab_table(soup)

    pool = DatabasePool.get()
    connection = pool.acquire()
    connection.current_schema = DB_SCHEMA
    cursor = connection.cursor()
    pass


if __name__ == "__main__":
    main()
    print("Done.")
