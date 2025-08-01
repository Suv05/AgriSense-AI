import requests
import pandas as pd
from bs4 import BeautifulSoup

url="https://pmddky.com/pmddky-district-list/"
response = requests.get(url)
html_content = response.text

soup = BeautifulSoup(html_content, "html.parser")

tables = soup.find_all("table", class_="has-fixed-layout")

# Assuming the correct table is still the third one (index 2)
rows = tables[2].find_all("tr")

all_districts = []

print("Districts in PMDDKY:")
for row in rows:
    cols = row.find_all("td")
    # Check if the row has enough columns (at least 2)
    if len(cols) > 1:
        # The second column (index 1) contains the district names
        district_str = cols[1].get_text(strip=True)
        # Split the string by ", " to get individual districts
        districts = [d.strip() for d in district_str.split(',')]
        all_districts.extend(districts)

# Remove any empty strings that might have been created by the split
all_districts = [d for d in all_districts if d]

print(all_districts)