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
and we save this to a pkl file for consumption by other scripts
and as an xls for review by humans
'''

from bs4 import BeautifulSoup
import requests
import pandas as pd
import os
from constants import path, file_for_scrape, file_for_metadata, xlsx_for_scrape_result, \
    SOURCE_KEY, LOCATION, metadata_reduced_columns

urls = [
    'https://www.eia.gov/dnav/pet/pet_sum_sndw_dcus_r10_w.htm',  # padd1
    'https://www.eia.gov/dnav/pet/pet_sum_sndw_dcus_r20_w.htm',  # padd2
    'https://www.eia.gov/dnav/pet/pet_sum_sndw_dcus_r30_w.htm',  # padd3
    'https://www.eia.gov/dnav/pet/pet_sum_sndw_dcus_r40_w.htm',  # padd4
    'https://www.eia.gov/dnav/pet/pet_sum_sndw_dcus_r50_w.htm',  # padd5
    'https://www.eia.gov/dnav/pet/pet_sum_sndw_dcus_nus_w.htm',  # us
]

# dataframe columns
scrape_columns = ['text', 'level', SOURCE_KEY, 'year_start', 'year_end']
report_columns = scrape_columns + ['level_change', 'symbol_list', 'full_name']
report_columns.remove(SOURCE_KEY)  # because its the index now


def get_soups_for_urls(urls):
    soups = []
    for url in urls:
        html = requests.get(url).content
        soup = BeautifulSoup(html, 'html.parser')
        soups.append(soup)
    return soups


def get_soup_to_df(soup):
    # access html data cells
    tds = soup.find_all('td', {'class': 'DataStub1'})

    # find the text and the indent
    texts = [str(td.contents[0]) for td in tds]
    indents = [td.find_previous('td').get('width') for td in tds]
    indents = [int(ii) for ii in indents]

    # theres a link at the end of each row with some useful data
    hyperlinks = [td.find_next('a') for td in tds]

    # get the source key from the url; this should be a regex but for now we're cheating with string slicing
    urls = [hyperlink.get('href') for hyperlink in hyperlinks]
    start, end = 32, -4
    source_keys = [url[start:end] for url in urls]

    # a year applies string is part of the link text
    year_ranges = [list(hyperlink.strings)[0] for hyperlink in hyperlinks]
    year_ranges = [year_range.split('-') for year_range in year_ranges]
    year_ranges = [[int(year_range[0]), int(year_range[1])] for year_range in year_ranges]

    # turn the indents into levels and then create a mapping lookup dict
    uniques = sorted(list(set(indents)))
    levels = list(range(len(uniques)))
    level_lookup = dict(zip(uniques, levels))

    # join it all up
    data = zip(texts,
               [level_lookup[indent] for indent in indents],
               source_keys,
               [year_range[0] for year_range in year_ranges],  # year_start
               [year_range[1] for year_range in year_ranges],  # year_end
               )

    # create and return a dataframe
    df = pd.DataFrame(data=list(data), columns=scrape_columns)
    df.set_index(SOURCE_KEY, drop=True, inplace=True)
    return df


def build_hierarchy_from_indent(df):
    # first order difference
    df['level_change'] = df['level'] - df['level'].shift(periods=1, axis='index', fill_value=0)

    # initialise column to capture full path as a list of symbols
    df['symbol_list'] = None
    df['symbol_list'] = df['symbol_list'].astype('object')

    symbol_list = []
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
                symbol_list.pop()
            except IndexError as err:
                pass

        # update the list and the df
        symbol_list.append(i)
        df.at[i, 'symbol_list'] = symbol_list.copy()
    return df


def build_flat_hierarchy_from_list(df):
    max_depth = df['symbol_list'].apply(len).max()
    hierarchy_df = pd.DataFrame(data=None, index=df.index, columns=range(max_depth))
    for i, row in df.iterrows():
        padded_leaves = row['symbol_list'] + [''] * (max_depth - len(row['symbol_list']))
        hierarchy_df.loc[i] = padded_leaves
    return hierarchy_df


def build_hierarchy_name_from_list(df):
    for i, row in df.iterrows():
        # create a temporary df using the symbols in the symbol list
        index = pd.Index(row['symbol_list'], name=SOURCE_KEY)
        temp_df = pd.DataFrame(index=index)
        # inner join and get the text as a list
        texts = df.join(temp_df, how='inner')['text'].values
        texts = list(texts)
        # concatenate into a full name
        df['full_name'] = (' | ').join(texts).copy()
    return df


def build_report_df(hierarchy_df, metadata_df, flat_hierarchy_df):
    # define columns
    columns = report_columns \
              + list(metadata_reduced_columns) \
              + list(flat_hierarchy_df.columns)

    # join and return the new df
    report_df = hierarchy_df.join(metadata_df).join(flat_hierarchy_df)
    return report_df[columns]


if __name__ == '__main__':
    # get saved metadata
    pathfile = os.path.join(path, file_for_metadata)
    metadata_df = pd.read_pickle(pathfile)

    # simple parser
    soups = get_soups_for_urls(urls)
    dfs = []
    for soup in soups:
        dfs.append(get_soup_to_df(soup))

    # derive hierarchy from indent as a level number
    hierarchy_df = pd.DataFrame()
    for df in dfs:
        new_df = build_hierarchy_from_indent(df)
        hierarchy_df = pd.concat([hierarchy_df, new_df], axis='rows')

    # add a column for the full name using the symbol list in each row
    hierarchy_df = build_hierarchy_name_from_list(hierarchy_df)

    # prepare for upload by building a flat hierarchy version w integer column names
    flat_hierarchy_df = pd.DataFrame()
    for df in dfs:
        new_df = build_flat_hierarchy_from_list(df)
        flat_hierarchy_df = pd.concat([flat_hierarchy_df, new_df], axis='rows')

    # join everything up
    report_df = build_report_df(hierarchy_df, metadata_df, flat_hierarchy_df)

    # save to file
    pathfile = os.path.join(path, file_for_scrape)
    report_df.to_pickle(pathfile)

    # save as xls
    pathfile = os.path.join(path, xlsx_for_scrape_result)
    with pd.ExcelWriter(pathfile) as writer:
        report_df.to_excel(writer, sheet_name='scrape_result')
