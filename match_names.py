import pandas as pd
import re
from fuzzywuzzy import process

def append_acronyms_advanced(df, column_name, reference_column):
      roman_numeral_pattern = re.compile(r'\b(?:I{1,3}|IV|V|VI{0,3}|IX|X{1,3}|XL|L|LX{0,3}|XC|C{1,3}|CD|D|DC{0,3}|CM|M{1,3})\b')
      def acronymize(text):
            words = text.split()
            for i, word in enumerate(words):
                  if roman_numeral_pattern.match(word):
                        acronym = ''.join([w[0].upper() for w in words[:i]])
                        return acronym + ' ' + ' '.join(words[i:])
            return text
      new_rows = []
      for _, row in df.iterrows():
            acronymized = acronymize(row[column_name])
            if acronymized != row[column_name]:
                  new_row = row.copy()
                  new_row[column_name] = acronymized
                  new_row[reference_column] = row[reference_column]
                  new_rows.append(new_row)
      df = df.append(new_rows, ignore_index=True)
      return df

firms = pd.read_csv("Downloads/funds.csv", header = None)
funds = pd.read_csv("Downloads/yaleinv.csv", header = None)

firms = append_acronyms_advanced(firms, '0', '1')

def find_closest_match(query, choices, firms):
      result = process.extractOne(query, choices)
      print(str(result[0]) + " " + str(result[1]) + " " + str(firms.iloc[result[2], 0]))
      return result[0], result[1], firms.iloc[result[2], 0] if result else (None, None, None)
funds[['Closest Match', 'simscore', 'Manager']] = funds[funds.columns[0]].apply(lambda x: pd.Series(find_closest_match(x, firms[firms.columns[1]], firms)))
funds.to_csv("matched_abbrev.csv")