from bs4 import BeautifulSoup
import requests
import oci
import urllib.request


def get_data(url):
    r = requests.get(url)
    return r.text

def grab_table():
	table = []
	for row in soup.find_all('tr'):
		lst = []
		for col in row.find_all('td'):
			lst += [col]
		table += [lst]
	table = table[3:] # remove non data rows
	table = [item[1:] for item in table] # remove icon from columns
	return table

base_url = "https://is.sci.gsfc.nasa.gov/gsfcdata/jpss1/viirs/level2/"
qparams = "?C=S;O=A"

files = []
file_names = []
htmldata = get_data(base_url + qparams)
soup = BeautifulSoup(htmldata, 'html.parser')
for item in soup.find_all('a'):
    file_name = item['href']
    if file_name.find(".txt") != -1:
        file = get_data(base_url + file_name)
        files += [file]
        file_names += [file_name]
