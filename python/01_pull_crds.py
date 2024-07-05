from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import numpy as np
import zipfile
import os
import io
from datetime import datetime

# Headers
headers = {
    "Accept-Language": "en-US,en;q=0.9,de;q=0.8,fr;q=0.7", 
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
  }

# Function to download the zip file and extract its contents
def download_excel(url, extract_to='extracted'):
    response = requests.get(url, headers = headers)
    if response.status_code == 200:
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall(extract_to)
            return z.namelist()
    else:
        raise Exception(f"Failed to download file from {url}")

# Function to extract the first column of each excel file in a directory
def extract_crds(directory):
    crds = {}
    for filename in os.listdir(directory):
        if filename.endswith('.xlsx'):
            file_path = os.path.join(directory, filename)
            df = pd.read_excel(file_path)
            if df.shape[1] > 1:
                crds[filename] = df.iloc[:, 1].tolist()
            else:
                crds[filename] = None 
    crds = pd.DataFrame(crds[list(crds.keys())[0]] + crds[list(crds.keys())[1]])
    return crds

# URL with SEC spreadsheets of (basic) investment manager data
webpage_url = 'https://www.sec.gov/data-research/sec-markets-data/foiadocsinvafoia' 

response = requests.get(webpage_url, headers = headers)
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

# Write CRDs to file
crds.to_csv("data/" + datetime.now().strftime("%Y-%m") + "/crds.csv")

