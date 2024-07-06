# SEC Private Fund-Firm Manager Database (sec-form-adv)
### Date: July 6, 2024

This is an in-development tool that concatenates all registration filings in the Investment Advisor Public Disclosure (IAPD) database into a single dataset of all disclosed private funds and their associated investment firms. Researchers, journalists, and activists can use the constructed dataset to identify the firms behind anonymously-named private funds of interest. Documentation forthcoming. 

This tool was built by divestment organizers opposing investments by institutions of higher education in industries of death and destruction, including those which enable Israel's ongoing genocide in Palestine. We support the use of technology for liberatory, constructive, and life-affirming ends, [not for apartheid, repression, and ethnic cleansing](https://www.notechforapartheid.com/).

## Repository structure

`data` stores all data produced, including the master dataset of fund-firm pairings (`data/XX-20XX/funds.csv`). The dataset is reconstructed on a monthly basis, and all previous versions are archived.


`python` stores all code used to construct the dataset. `01_pull_crds.py` retrieves all active Central Registration Depository (CRD) identifiers from the SEC. `02_scrape_iapd.py` pulls the registration filing for each firm CRD from the IAPD and constructs the master dataset of fund-firm pairings. `03_match_names.py` is an example of an application for the dataset, in which a list of LP names (obtained from Yale University's tax filings) are fuzzily-matched against the fund-firm dataset to reveal some of the investment partners behind the Yale endowment.


`.github/workflows` stores the two key automated actions that build the dataset on a monthly basis. `pull_crds.yml` automates `python/01_pull_crds.py`, and `build.yml` automates `02_scrape_iapd.py`.

## Application: Yale University endowment

*TODO: Write working example*

----------

```
python3 crds.py
```

Matching to existing names

```
firms = pd.read_csv("Downloads/funds.csv", header = None)
funds = pd.read_csv("Downloads/yaleinv.csv", header = None)
def find_closest_match(query, choices, firms):                               
      result = process.extractOne(query, choices)
      print(str(result[0]) + " " + str(result[1]) + " " + str(firms.iloc[result[2], 0]))
      return result[0], result[1], firms.iloc[result[2], 0] if result else (None, None, None)
funds[['Closest Match', 'simscore', 'Manager']] = funds[funds.columns[0]].apply(lambda x: pd.Series(find_closest_match(x, firms[firms.columns[1]], firms)))
funds.to_csv("matched.csv")
```
