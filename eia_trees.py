import os
import pandas as pd
import matplotlib.pyplot as plt
import networkx as nx
import datetime as dt
from constants import path, TAB_DESCRIPTION, LOCATION, file_for_scrape_result, \
    file_for_cleaned_metadata, xlsx_scrape_w_leaf_nodes, file_for_scrape_w_leaf_nodes
from eia_hierarchy_definitions import manually_remove_symbols


def build_graph_per_row_of_df(df):
    for i, row in df.iterrows():
        # get the list of nodes
        symbol_list = row['symbol_list']
        # define edges as tuple-pairs of nodes being a (start, end) format
        edges = [(a, b) for a, b in zip(['root'] + symbol_list[:-1], symbol_list)]
        # create an empty graph and add edges
        G = nx.Graph()
        G.add_edges_from(edges)
        # add graph to df
        df.at[i, 'graph'] = G


def build_graph_per_report_location(df):
    H = nx.Graph()
    for G in df['graph'].values:
        H.update(G)
    return H


def build_all_tree_analysis():
    # ======================================
    # import the scrape result
    pathfile = os.path.join(path, file_for_scrape_result)
    scrape_df = pd.read_pickle(pathfile)
    scrape_columns = scrape_df.columns

    # import the metadata
    pathfile = os.path.join(path, file_for_cleaned_metadata)
    metadata_df = pd.read_pickle(pathfile)

    # ======================================
    # we dont want expired symbols
    current_year = dt.date.today().year

    # need these columns from the metadata to group the trees
    enrich_columns = [TAB_DESCRIPTION, LOCATION]

    # ======================================
    # and now the graph analysis

    # this new df will hold a full tree per table/location
    tree_df = metadata_df[enrich_columns].drop_duplicates(ignore_index=True).copy(deep=True)
    tree_df.set_index(enrich_columns, drop=True, inplace=True)

    # how tiresome
    tree_df['graph'] = None
    tree_df['graph'] = tree_df['graph'].astype('object')
    tree_df['cleaned_graph'] = None
    tree_df['cleaned_graph'] = tree_df['cleaned_graph'].astype('object')
    tree_df['leaf_nodes'] = None
    tree_df['leaf_nodes'] = tree_df['leaf_nodes'].astype('object')

    scrape_df['graph'] = None
    scrape_df['graph'] = scrape_df['graph'].astype('object')

    # enrich the scrape data with table/location columns from the metadata
    scrape_df = scrape_df.join(metadata_df[enrich_columns])
    # were going to populate this new column
    scrape_df.insert(2, 'leaf_node', False)

    for i, row in tree_df.iterrows():
        # filter the source for the table/location
        mask = scrape_df[enrich_columns] == i
        mask = mask.all(axis='columns')
        df = scrape_df.loc[mask]

        # add the line graph
        build_graph_per_row_of_df(df)

        # use each line graph from the df to build the full tree
        H = build_graph_per_report_location(df)
        tree_df.at[i, 'graph'] = H

        # and now the cleaned graph
        I = H.copy()

        def _remove_node(I, source_key):
            try:
                I.remove_node(source_key)
            except nx.exception.NetworkXError as e:
                print(source_key, e)

        # manual override of hierarchy
        for source_key in manually_remove_symbols:
            _remove_node(I, source_key)

        # remove nodes that have expired
        mask = df['year_end'] != current_year
        for source_key in df.index[mask]:
            _remove_node(I, source_key)

        tree_df.at[i, 'cleaned_graph'] = I

        # leaf nodes only have one neighbour; neighbours are reported from adjacency dict
        leaf_nodes = [n for n in I.nodes if len(I.adj[n]) == 1]

        # enrich the tree df as a list per row
        tree_df.at[i, 'leaf_nodes'] = leaf_nodes

        # enrich the scrape df with a boolean label per row
        mask = scrape_df.index.isin(leaf_nodes)
        scrape_df.loc[mask, 'leaf_node'] = True

    # ======================================
    # save the data

    # save to a pickle just in case
    pathfile = os.path.join(path, file_for_scrape_w_leaf_nodes)
    tree_df.to_pickle(pathfile)

    # save to csv for upload to mosaic
    pass

    # retain the order and exclude enrichment
    columns = [c for c in scrape_df.columns if c in list(scrape_columns) + ['leaf_node']]

    # and save to xls so we can use in tableau
    pathfile = os.path.join(path, xlsx_scrape_w_leaf_nodes)
    with pd.ExcelWriter(pathfile) as writer:
        scrape_df[columns].to_excel(writer, sheet_name='scrape_result')

    # ======================================
    # and view the US stocks tree as a chart

    tree = ('Stocks', 'U.S.')
    G = tree_df.loc[tree, 'cleaned_graph']
    leaf_nodes = tree_df.loc[tree, 'leaf_nodes']
    node_color = ['red' if node in leaf_nodes else 'gray' for node in G.nodes]
    ax = plt.subplot(111)
    nx.draw(G, node_color=node_color, with_labels=True)
    plt.show()


if __name__ == '__main__':
    build_all_tree_analysis()



