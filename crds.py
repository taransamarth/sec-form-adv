from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from multiprocessing import Pool

# Get link to the Form ADV, given a CRD
def get_manager_sec_adv_actual_url(crd = 107322, url=None):
    if crd is None and url is None:
      raise ValueError('Need crd (arg1) or selector URL (arg2)')
    if crd is not None and url is None:
      url = 'https://files.adviserinfo.sec.gov/iapd/crd_iapd_AdvVersionSelector.aspx?ORG_PK=' + str(crd)
    response = requests.get(url)
    actual_url = response.url
    return actual_url

# Parse a page
def get_html_page(url='https://files.adviserinfo.sec.gov/IAPD/content/viewform/adv/sections/iapd_AdvIdentifyingInfoSection.aspx?ORG_PK=107322&FLNG_PK=00CCBCEE000801D30066B11104991C75056C8CC0'):
    response = requests.get(url)
    page = BeautifulSoup(response.content, 'html.parser')
    return page

# Parse manager name from page
def get_mgr_name(page):
  mgr_name = page.select_one('.summary-displayname').get_text()
  return mgr_name

# Get url to the private fund section of Form ADV
def parse_pf_url(crd = 107322, url=None, manager=None, return_wide=False):
    if crd is None and url is not None:
      idCRD = int(url.split('?ORG_PK=')[1])
    elif crd is not None and url is None:
      idCRD = crd
    else:
      raise ValueError('Need crd (arg1) or selector URL (arg2)')
    actual_url = get_manager_sec_adv_actual_url(idCRD)
    url_primary_key = actual_url.split('FLNG_PK=')[1]
    page = get_html_page(actual_url)
    base_url = actual_url.split('sections')[0]
    mgr_name = get_mgr_name(page)
    secs = [base_url[:-1] + item['href'][2:] for item in page.select('.sidebar a[href^=".."]')]
    pf_sec = re.sub('\s+',' ',[i for i in secs if "Private" in i][0]).strip()
    return [mgr_name, pf_sec]

# Parse PFs from ADV
def collect_pf_data(pf_url = "https://files.adviserinfo.sec.gov/IAPD/content/viewform/adv/Sections/iapd_AdvPrivateFundReportingSection.aspx?ORG_PK=107322&FLNG_PK=00CCBCEE000801D30066B11104991C75056C8CC0"):
    main = get_html_page(pf_url).select('.main div')
    all_div_ids = [div['id'] for div in main if div.has_attr('id')]
    all_div_ids = list(set(filter(None, all_div_ids)))
    page_table_nodes = [f"#{div_id}" for div_id in all_div_ids if re.search("pnlFund", div_id)]
    content = BeautifulSoup(requests.get(pf_url).content, 'html.parser')
    fund_names = []
    for id in page_table_nodes:
      div = content.find("div", {"id": str(id[1:])})
      fund_name = div.select_one('td:-soup-contains("Name of the") span')
      if fund_name is not None:
        fund_name = fund_name.get_text()
      else:
        fund_name = "Unidentified"
      fund_names += [fund_name]
    return fund_names

# Convert file of CRDS to list
def file_to_list(name):
  lst = pd.read_csv(name)
  return lst[lst.columns[2]].values.tolist()

def harvest_fund_names_wrapper(crd):
    df2 = pd.DataFrame()
    try:
      pfurl = parse_pf_url(crd)
      mgr_name = pfurl[0]
      pfurl = pfurl[1]
      names = collect_pf_data(pfurl)
      if len(names) > 0:
        df2 = pd.DataFrame({'Firm': [mgr_name] * len(names), 'Fund': names})
      else:
        df2
      print(str(crd) + " has " + str(len(names)) + " funds.")
    except:
      print(str(crd) + " threw an error.")
    return df2

def concat_dataframes(df_list):
  non_empty_dfs = [df for df in df_list if not df.empty]
  result = pd.concat(non_empty_dfs, ignore_index=True)
  return result

def harvest_fund_names_parallel(crd_list):
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(harvest_fund_names_wrapper, crd_list))
    # df = concat_dataframes(results)
    return results

if __name__ == '__main__':
    crds = pd.read_csv("newcrds.csv")
    crds = list(crds['x'])
    df_parallel = harvest_fund_names_parallel(crds)
    for df in df_parallel:
      print("Concatenating a dataframe...")
      df.to_csv("funds.csv", mode = 'a', header = None, index = False)
