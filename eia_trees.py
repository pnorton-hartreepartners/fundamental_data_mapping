import os
import pandas as pd
import matplotlib.pyplot as plt
import networkx as nx
import datetime as dt
from constants import path

SOURCE_KEY = 'Sourcekey'
TAB_DESCRIPTION = 'TabDescription'
LOCATION = 'Location'
UNIT = 'Unit'
DESCRIPTION = 'Description'
file_for_scrape_result = 'eia-weekly-scrape-result.pkl'
file_for_cleaned_metadata = 'eia-weekly-metadata-cleaned.pkl'

# ======================================
# import the scrape result
pathfile = os.path.join(path, file_for_scrape_result)
scrape_df = pd.read_pickle(pathfile)

# import the metadata
pathfile = os.path.join(path, file_for_cleaned_metadata)
metadata_df = pd.read_pickle(pathfile)

# join it up
scrape_df = scrape_df.join(metadata_df)

# ======================================
# filter for headline stocks data ie level zero

# we dont want expired symbols
current_year = dt.date.today().year

# select US stocks
mask = scrape_df[TAB_DESCRIPTION].eq('Stocks') \
       & scrape_df['year_end'].eq(current_year) \
       & scrape_df[LOCATION].eq('U.S.')
df = scrape_df[mask].copy(deep=True)

# define a graph for each symbol_list and save to df
df['graph'] = None
df['graph'] = df['graph'].astype('object')
for i, row in df.iterrows():
    # create empty tree
    G = nx.Graph()
    # get the list of nodes
    symbol_list = row['symbol_list']
    # define edges as tuple-pairs of nodes being a (start, end) format
    edges = [(a, b) for a, b in zip(['root'] + symbol_list[:-1], symbol_list)]
    # add edges to the tree
    G.add_edges_from(edges)
    # add graph to df
    df.at[i, 'graph'] = G

# use each tree from the df to build the full tree
H = nx.Graph()
for G in df['graph'].values:
    H.update(G)

# leaf nodes only have one neighbour; neighbours are reported from adjacency dict
leaf_nodes = [n for n in H.nodes if len(H.adj[n]) == 1]

# save to a df and xls so we can use in tableau
index = pd.Index(leaf_nodes, name=SOURCE_KEY)
leaf_df = pd.DataFrame(index=index)

# save as xls
xlsx_for_leaf_nodes = 'eia_weekly_leaf_nodes.xlsx'
pathfile = os.path.join(path, xlsx_for_leaf_nodes)
with pd.ExcelWriter(pathfile) as writer:
    leaf_df.to_excel(writer, sheet_name='leaf_nodes')

# view the tree
ax = plt.subplot(111)
nx.draw(H, with_labels=True)
plt.show()

print()
pass
