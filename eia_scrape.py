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
import numpy as np
import os
from constants import path, file_for_scrape_result, xlsx_for_scrape_result, \
    SOURCE_KEY, file_for_cleaned_metadata, TAB_DESCRIPTION, LOCATION, csv_for_hierarchy_result, xlsx_for_mapping_result, \
    xlsx_for_mapping_errors, numbers_as_words
from eia_hierarchy_definitions import manually_remove_texts

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

leaf = 'leaf'
branch = 'branch'


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


def build_flat_hierarchy_from_list(df, remove_texts=True):
    label = 'text'
    mapper = dict(zip(df.index, df[label].values))
    max_depth = df['symbol_list'].apply(len).max()
    columns = numbers_as_words[:max_depth]
    hierarchy_df = pd.DataFrame(data=None, index=df.index, columns=columns)
    for i, row in df.iterrows():
        mosaic_list = row['symbol_list']
        # map symbol to text
        mosaic_list = list(map(lambda x: mapper[x], mosaic_list))
        if remove_texts:
            # remove according to config list
            mosaic_list = [e for e in mosaic_list if e not in manually_remove_texts]
        # add the columns of data
        hierarchy_df.loc[i] = mosaic_list + [''] * (max_depth - len(mosaic_list))
    return hierarchy_df


def build_hierarchy_name_from_list(df):
    for i, row in df.iterrows():
        # create a temporary df using the symbols in the symbol list
        index = pd.Index(row['symbol_list'], name=SOURCE_KEY)
        temp_df = pd.DataFrame(index=index)
        # inner join and get the text as a list
        texts = df.join(temp_df, how='inner')['text'].values
        texts = list(texts)
        # concatenate into a full name and update
        full_name = (' | ').join(texts)
        df.loc[i, 'full_name'] = full_name
    return df


def build_all_scrape():
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

    # prepare for mosaic upload by building a flat hierarchy version w integer column names
    flat_hierarchy_df = pd.DataFrame()
    for df in dfs:
        new_df = build_flat_hierarchy_from_list(df, remove_texts=False)
        flat_hierarchy_df = pd.concat([flat_hierarchy_df, new_df], axis='rows')

    # join it up
    report_df = hierarchy_df.join(flat_hierarchy_df)

    # save to file
    pathfile = os.path.join(path, file_for_scrape_result)
    report_df.to_pickle(pathfile)

    # we need to enrich scrape data with table/location for human consumption of xls
    pathfile = os.path.join(path, file_for_cleaned_metadata)
    metadata_df = pd.read_pickle(pathfile)

    extra_columns = [TAB_DESCRIPTION, LOCATION]
    report_df = report_df.join(metadata_df[extra_columns])
    columns = list(hierarchy_df.columns) + extra_columns + list(flat_hierarchy_df.columns)
    report_df = report_df[columns]

    pathfile = os.path.join(path, xlsx_for_scrape_result)
    with pd.ExcelWriter(pathfile) as writer:
        report_df.to_excel(writer, sheet_name='scrape_result')


def get_mapping_df(df):
    index = pd.Index(data=[], name=SOURCE_KEY)
    mapping_df = pd.DataFrame(data=None, index=index, columns=[leaf, branch])
    for i, row in df.iterrows():
        # replace empty string and drop; drop method fails if index is missing
        row.replace(to_replace='', value=np.NaN, inplace=True)
        row.dropna(inplace=True)
        # find the rightmost column index and value for leaf
        max_column = row.index[-1]
        max_value = row.loc[max_column]
        mapping_df.at[i, leaf] = f'{max_column}:{max_value}'
        # create branch syntax using all nodes
        branch_str = '@'.join(row.values)
        mapping_df.at[i, branch] = f'{max_column}:{branch_str}'
    return mapping_df


if __name__ == '__main__':
    # build_all_scrape()

    pathfile = os.path.join(path, file_for_scrape_result)
    report_df = pd.read_pickle(pathfile)

    # make a copy of the df and remove old columns
    first_column_name = 0
    first_column_index = report_df.columns.get_loc(first_column_name)
    report_df = report_df[report_df.columns[:first_column_index]]

    # and rebuild with option to remove text
    report_df = build_flat_hierarchy_from_list(report_df, remove_texts=False)

    # for upload to mosaic
    mapping_df = get_mapping_df(report_df)
    hierarchy_df = report_df.drop_duplicates(ignore_index=True)

    # collect the errors generated when we upload using leaf format
    pathfile = os.path.join(path, xlsx_for_mapping_errors)
    errors_df = pd.read_excel(pathfile)

    # and now use these source_keys to select the right upload format
    error_keys = errors_df[SOURCE_KEY].values

    # munge alert
    mask = mapping_df.index.isin(error_keys)
    branch_df = mapping_df.loc[mask, branch]
    leaf_df = mapping_df.loc[~mask, leaf]
    mapping2_df = pd.concat([leaf_df, branch_df], axis='index')

    # save the hierarchy; smash the patriarchy
    pathfile = os.path.join(path, csv_for_hierarchy_result)
    hierarchy_df.to_csv(pathfile, index=False)

    # save the mapping; for some reason this needs to be xlsx!
    pathfile = os.path.join(path, xlsx_for_mapping_result)
    with pd.ExcelWriter(pathfile) as writer:
        mapping_df.to_excel(writer, sheet_name='mapping')

