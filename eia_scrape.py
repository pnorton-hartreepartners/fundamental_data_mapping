'''
this script scrapes the eia weekly report directly from the web
in order to capture the indentation that defines the hierarchy
the first output is dataframe with:
- text description
- indent level
- indent change
- symbol
- the cumulative text including those from previous levels in the hierarchy

the second output is a dataframe with:
- the tree of symbols as a flat structure ie one column per tree level
- some metadata
and we save this to a pkl file
'''

from bs4 import BeautifulSoup
import requests
import pandas as pd
import os
from constants import path, file_for_scrape, file_for_metadata, SOURCE_KEY

url = r'https://www.eia.gov/dnav/pet/pet_sum_sndw_dcus_nus_w.htm'
html = requests.get(url).content
soup = BeautifulSoup(html, 'html.parser')

# access html data cells
tds = soup.find_all('td', {'class': 'DataStub1'})

# find the text and the indent
texts = [str(td.contents[0]) for td in tds]
indents = [td.find_previous('td').get('width') for td in tds]
indents = [int(ii) for ii in indents]

# find the source key; this should be a regex
urls = [td.find_next('a').get('href') for td in tds]
start, end = 32, -4
source_keys = [url[start:end] for url in urls]

# turn the indents into levels and then create a mapping lookup dict
uniques = sorted(list(set(indents)))
levels = list(range(len(uniques)))
level_lookup = dict(zip(uniques, levels))

# join it all up
triples = zip(texts, [level_lookup[indent] for indent in indents], source_keys)
triples = list(triples)

# create a dataframe to set up the analysis
columns = ['text', 'level', 'source_key']
df = pd.DataFrame(data=triples, columns=columns)

# ==================================================
# get saved metadata
pathfile = os.path.join(path, file_for_metadata)
metadata_df = pd.read_pickle(pathfile)

# ==================================================
# create hierarchy as a list in a single column

# first order difference
df['level_change'] = df['level'] - df['level'].shift(periods=1, axis='index', fill_value=0)

# initialise column and change type to accept list
df['hierarchy_text'] = None
df['hierarchy_text'] = df['hierarchy_text'].astype('object')
df['hierarchy_symbol'] = None
df['hierarchy_symbol'] = df['hierarchy_symbol'].astype('object')

text_list, symbol_list = [], []
for i, row in df.iterrows():
    # extend the list ie dont pop
    if row['level_change'] >= 1:
        pop_count = 0

    # even if the list length is unchanged, we pop once to replace with the new value
    else:
        pop_count = abs(row['level_change']) + 1

    # pop the required number of times
    for _ in range(pop_count):
        try:
            text_list.pop()
            symbol_list.pop()
        except IndexError as err:
            pass

    # update the lists and the df
    text_list.append(row['text'])
    symbol_list.append(row['source_key'])
    df.at[i, 'hierarchy_text'] = text_list.copy()
    df.at[i, 'hierarchy_symbol'] = symbol_list.copy()

df.set_index('source_key', drop=True, inplace=True)

# ==================================================
# create hierarchy this time as a flat dataframe of symbols

max_depth = df['hierarchy_symbol'].apply(len).max()
hierarchy_df = pd.DataFrame(data=None, index=df.index, columns=range(max_depth))
for i, row in df.iterrows():
    padded_leaves = row['hierarchy_symbol'] + [''] * (max_depth - len(row['hierarchy_symbol']))
    hierarchy_df.loc[i] = padded_leaves

# ==================================================
# join everything up

tree_columns = hierarchy_df.columns
meta_columns = metadata_df.columns
scrape_columns = ['text', 'level', 'level_change']

mask = metadata_df['Location'] == 'U.S.'
metadata_df = metadata_df[mask]
report_df = hierarchy_df.join(metadata_df).join(df)
report_df = report_df[scrape_columns + list(meta_columns) + list(tree_columns)]

# ==================================================
# save to file

pathfile = os.path.join(path, file_for_scrape)
report_df.to_pickle(pathfile)

report_df.to_clipboard()
print('hello world')


