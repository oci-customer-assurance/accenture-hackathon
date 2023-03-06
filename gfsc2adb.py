from bs4 import BeautifulSoup
import requests
import urllib.request
import csv
import time

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

def append_file(soup): # old, not used
    for item in soup.find_all('a'):
        file_name = item['href']
        if file_name.find(".txt") != -1:
            file = get_data(base_url + file_name)
            files += [file]
            file_names += [file_name]

base_url = "https://is.sci.gsfc.nasa.gov/gsfcdata/jpss1/viirs/level2/"
qparams = "?C=S;O=A"
htmldata = get_data(base_url + qparams)
soup = BeautifulSoup(htmldata, 'html.parser')

#files, file_names = append_file(soup)
table = grab_table(soup)
with open(f"data_{get_time()}.csv", "w") as f:
    csv_writer = csv.writer(f, delimiter = ",")
    csv_writer.writerow(["File name", "Last Modified", "Size", "Description"])
    for row in table:
        csv_writer.writerow(row)
