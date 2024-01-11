# sec-form-adv

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
