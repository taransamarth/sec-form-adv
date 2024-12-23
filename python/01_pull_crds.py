from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import numpy as np
import zipfile
import os
import io
from datetime import datetime
import time
import requests_random_user_agent

# Function to download the zip file and extract its contents
def download_excel(url, extract_to='extracted_files'):
    response = requests.get(url)
    while(response.status_code != 200):
        response = requests.get(url)
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        z.extractall(extract_to)
        return z.namelist()

# Function to extract the first column of each excel file in a directory
def extract_crds(directory):
    crds = {}
    for filename in os.listdir(directory):
        if filename.endswith('.xlsx'):
            file_path = os.path.join(directory, filename)
            df = pd.read_excel(file_path)
            if df.shape[1] > 1:
                crds[filename] = df[df["Count of Private Funds - 7B(1)"] > 0].iloc[:, 1].tolist()
            else:
                crds[filename] = None 
    crds = pd.DataFrame(crds[list(crds.keys())[0]] + crds[list(crds.keys())[1]])
    return crds

# URL with SEC spreadsheets of (basic) investment manager data
webpage_url = 'https://www.sec.gov/data-research/sec-markets-data/information-about-registered-investment-advisers-exempt-reporting-advisers' 

s = requests.Session()
response = requests.get(webpage_url)
soup = BeautifulSoup(response.content, 'html.parser')

while soup(text=re.compile('Automated access')):
    print("Access blocked... waiting 10 seconds.")
    s = requests.Session()
    time.sleep(10)
    response = requests.get(webpage_url)
    soup = BeautifulSoup(response.content, 'html.parser')

# Find most recent links to .zip files (for registered and exempt advisers)
zip_links = [a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith('.zip')][:2]

# Directory to extract the zip files
extract_dir = 'extracted_files'
os.makedirs(extract_dir, exist_ok=True)

# Download and process 
for zip_link in zip_links:
    # Full URL
    full_url = requests.compat.urljoin(webpage_url, zip_link)
    print(f"Downloading and extracting {full_url}")
    
    # Download and extract the zip file
    extracted_files = download_excel(full_url, extract_dir)
    print(f"Extracted files: {extracted_files}")

# Extract the first column from each Excel file
crds = extract_crds(extract_dir)

# Directory to save the CRD file
date_dir = "data/" + datetime.now().strftime("%Y-%m")
os.makedirs(date_dir, exist_ok=True)

# Write CRDs to file
crds.to_csv(date_dir + "/crds.csv")

