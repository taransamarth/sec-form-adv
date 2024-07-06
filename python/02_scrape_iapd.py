from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from multiprocessing import Pool
from datetime import datetime

def get_manager_sec_adv_actual_url(crd = 107322, url=None):
    if crd is None and url is None:
      raise ValueError('Need crd (arg1) or selector URL (arg2)')
    if crd is not None and url is None:
      url = 'https://files.adviserinfo.sec.gov/iapd/crd_iapd_AdvVersionSelector.aspx?ORG_PK=' + str(crd)
    response = requests.get(url)
    actual_url = response.url
    return actual_url

def get_html_page(url='https://files.adviserinfo.sec.gov/IAPD/content/viewform/adv/sections/iapd_AdvIdentifyingInfoSection.aspx?ORG_PK=107322&FLNG_PK=00CCBCEE000801D30066B11104991C75056C8CC0'):
    response = requests.get(url)
    page = BeautifulSoup(response.content, 'html.parser')
    return page

def get_mgr_name(page):
  mgr_name = page.select_one('.summary-displayname').get_text()
  return mgr_name

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

def collect_pf_data(pf_url = "https://files.adviserinfo.sec.gov/IAPD/content/viewform/adv/Sections/iapd_AdvPrivateFundReportingSection.aspx?ORG_PK=107322&FLNG_PK=00465EE2000801D70086952104D67719056C8CC0"):
    main = get_html_page(pf_url).select('.main div')
    all_div_ids = [div['id'] for div in main if div.has_attr('id')]
    all_div_ids = list(set(filter(None, all_div_ids)))
    page_table_nodes = [f"#{div_id}" for div_id in all_div_ids if re.search("pnlFund", div_id)]
    content = BeautifulSoup(requests.get(pf_url).content, 'html.parser')
    df = pd.DataFrame(columns=['Fund', 'id', 'juris', 'type', 'gross', 'owners'])
    for id in page_table_nodes:
      div = content.find("div", {"id": str(id[1:])})
      spans = div.select('span')
      fund_name = spans[0].get_text() if spans[0] is not None else "Unidentified"
      fund_number = spans[1].get_text() if spans[1] is not None else "Unidentified"
      juris = spans[3].get_text() if spans[1] is not None else "Unidentified"
      gross = re.sub(r'\D', '', div.find('td', string = lambda text: text and "11." in text).find_next('span').get_text()) if div.find('td', string = lambda text: text and "11." in text).find_next('span').text is not None else "Unidentified"
      owners = div.find('td', string = lambda text: text and "13." in text).find_next('span').get_text() if spans[19] is not None else "Unidentified"
      fund_type = div.find('td', string = lambda text: text and "10." in text).find_next('img', alt=lambda alt: alt and 'selected, changed' in alt).find_next_sibling(string=True).strip()
      df.loc[len(df)] = [fund_name, fund_number, juris, fund_type, gross, owners]
    return df

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
        names.insert(0, 'Firm', [mgr_name] * len(names))
        df2 = names
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
    date_dir = "data/" + datetime.now().strftime("%Y-%m")
    crds = pd.read_csv(date_dir + "/crds.csv")
    crds = list(crds['0'])
    df_parallel = harvest_fund_names_parallel(crds)
    for df in df_parallel:
      print("Concatenating a dataframe...")
      df.to_csv(date_dir + "/funds.csv", mode = 'a', header = None, index = False)


