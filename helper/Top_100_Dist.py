import pandas as pd
import requests
import time
import json # You need to import the json module for saving to a file
from dotenv import load_dotenv
import os

dist = [
    "Sitapur", "Bijnor", "Azamgarh", "Barabanki", "Rohtas", "Nalanda",
    "Gaya", "Madhubani", "Purnia", "Sehore", "Vidisha", "Hoshangabad",
    "Chhindwara", "Jabalpur", "Ludhiana", "Patiala", "Sangrur",
    "Bathinda", "Moga", "Karnal", "Hisar", "Kurukshetra", "Sirsa",
    "Fatehabad", "Sriganganagar", "Kota", "Baran", "Banswara", "Alwar",
    "Bastar", "Raipur", "Bilaspur", "Kanker", "Dhamtari", "Kalahandi",
    "Koraput", "Mayurbhanj", "Bargarh", "Malkangiri", "puri", "bhubaneswar",
    "Ranchi", "Gumla", "Palamu", "Simdega", "Hazaribagh", "Burdwan",
    "Bankura", "Murshidabad", "Malda", "Nadia", "Nagaon", "Barpeta",
    "Dhubri", "Golaghat", "Sivasagar", "Guntur", "Krishna", "Nellore",
    "Anantapur", "East Godavari", "Nizamabad", "Karimnagar", "Khammam",
    "Mahbubnagar", "Warangal", "Thanjavur", "Erode", "Salem", "Villupuram",
    "Coimbatore", "Mandya", "Tumakuru", "Belagavi", "Mysuru", "Raichur",
    "Wayanad", "Idukki", "Palakkad", "Kottayam", "Thrissur", "Rajkot",
    "Junagadh", "Surendranagar", "Sabarkantha", "Banaskantha", "Nashik",
    "Ahmednagar", "Jalgaon", "Nagpur", "Solapur", "Nainital", "Almora",
    "Haridwar", "Kangra", "Mandi", "Shimla", "Solan", "Kullu", "Goa",
    "Tripura", "Dhalai", "Sepahijala", "Imphal", "Thoubal", "Churachandpur",
    "Dimapur", "Kohima", "Mokokchung", "Aizawl", "Lunglei", "Sikkim",
    "Lohit", "Delhi", "Karaikal", "Anantnag", "Baramulla", "Kupwara",
    "Udhampur", "Leh", "Kargil", "Andaman", "Kavaratti", "Agatti",
    "Silvassa", "Daman", "Diu"
]

# Load environment variables
load_dotenv()
api_key = os.getenv("OPEN_WEATHER_API_KEY_1")

def fetch_data():
    json_data = [] # Initialize the list inside the function
    for district in dist:
        # Construct the URL
        url = f"https://api.openweathermap.org/data/2.5/weather?q={district},IN&appid={api_key}&units=metric"
        
        try:
            response = requests.get(url)
            response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
            data = response.json()
            
            # Process the data
            if "coord" in data:
                main_data = {
                    "name": district,
                    "lat": data["coord"]["lat"],
                    "lon": data["coord"]["lon"],
                }
                json_data.append(main_data) # Use append() for lists
                print(f"Successfully fetched data for {district}")
            else:
                print(f"Could not find coordinates for {district}")

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {district}: {e}")
            print(f"Status code: {response.status_code if 'response' in locals() else 'N/A'}")

        time.sleep(1) # To avoid hitting the API rate limit

    return json_data

# The main execution block
try:
    # Fetch the data
    data_list = fetch_data()
    
    # Check if data was fetched
    if data_list:
        # The output format you want is a list of dictionaries, which can be directly saved to JSON.
        with open("helper/config.json", "w") as outfile:
            json.dump(data_list, outfile, indent=2)
        
        print(f"Data fetched and saved to helper/config.json successfully. Total districts: {len(data_list)}")
    else:
        print("No data was fetched to save.")

except Exception as e:
    print(f"An error occurred: {e}")