'''
build the naming based on the scraped hierarchy of the Imports table for the US
make some adjustments to the naming
and save with the source key so we can build a mapping afterwards
then finally drop duplicates to create a hierarchy
'''

import os
import pandas as pd
import numpy as np
from constants import path, xlsx_for_manual_hierarchy, file_for_scrape_result, \
    SOURCE_KEY, TAB_DESCRIPTION, LOCATION, file_for_cleaned_metadata, numbers_as_words, csv_for_hierarchy_result, \
    file_for_mapping_preparation


def build_hierarchy_analysis():
    # collect manual mapping from xls
    x = pd.ExcelFile(xlsx_for_manual_hierarchy)
    sheets_dict = {sheet_name: x.parse(sheet_name) for sheet_name in x.sheet_names if sheet_name.startswith('all')}
    raw_mapping_df = pd.concat(sheets_dict.values(), axis='index')

    # need the scrape result for the hierarchy name
    pathfile = os.path.join(path, file_for_scrape_result)
    scrape_result_df = pd.read_pickle(pathfile)

    # need the metadata for the table names and locations
    pathfile = os.path.join(path, file_for_cleaned_metadata)
    metadata_df = pd.read_pickle(pathfile)

    # ================================================================
    # clean the raw data
    clean_mapping_df = raw_mapping_df.copy(deep=True)

    # rename column
    HIERARCHY_KEY = 'HierarchyKey'
    clean_mapping_df.rename(columns={'Imports': HIERARCHY_KEY}, inplace=True)

    # and drop this; because its manually added in xls and incomplete
    clean_mapping_df.drop(TAB_DESCRIPTION, axis='columns', inplace=True)

    # drop rows from the mapping table where the sourcekey is missing
    mask = pd.isnull(clean_mapping_df[SOURCE_KEY])
    clean_mapping_df = clean_mapping_df[~mask]

    # and finally add a trivial mapping for our Imports table
    # obviously this was missing from the xls
    mask1 = metadata_df[TAB_DESCRIPTION] == 'Imports'
    mask2 = metadata_df[LOCATION] == 'U.S.'
    data = metadata_df[mask1 & mask2].index
    columns = [HIERARCHY_KEY, SOURCE_KEY]
    imports_df = pd.DataFrame(data=np.array([data, data]).T, columns=columns)

    clean_mapping_df = pd.concat([clean_mapping_df, imports_df], axis='index')

    # ================================================================
    # get the original scrape hierarchy
    # and the proposed mapped hierarchy side-by-side

    # get the original scraped hierarchy, just for US location
    original_hierarchy_df = pd.merge(scrape_result_df, metadata_df[[TAB_DESCRIPTION, LOCATION]],
                                     left_index=True, right_index=True)
    mask_location = original_hierarchy_df[LOCATION] == 'U.S.'
    mask_table = original_hierarchy_df[TAB_DESCRIPTION] == 'Imports'
    columns = ['full_name', TAB_DESCRIPTION, LOCATION]

    # add the new proposed hierarchy, requested by the manual mapping, from the Imports table
    result_df1 = pd.merge(original_hierarchy_df.loc[mask_location & mask_table][columns],
                          clean_mapping_df,
                          how='left',
                          left_index=True, right_on=HIERARCHY_KEY)
    result_df1.rename(columns={'full_name': 'new_hierarchy'}, inplace=True)
    result_df1.set_index(SOURCE_KEY, drop=True, inplace=True)

    # and the original hierarchy from the web scrape
    result_df2 = pd.merge(original_hierarchy_df.loc[mask_location, columns],
                          clean_mapping_df,
                          how='left',
                          left_index=True, right_on=SOURCE_KEY)
    result_df2.rename(columns={'full_name': 'original_hierarchy'}, inplace=True)
    result_df2.set_index(SOURCE_KEY, drop=True, inplace=True)

    # join the two resultsets
    result_df = pd.merge(result_df1['new_hierarchy'], result_df2,
                         how='outer',
                         left_index=True, right_index=True)

    # outer join results in nan
    # so we need to label them as missing to fix later
    result_df['missing'] = False
    mask = pd.isnull(result_df['new_hierarchy'])
    result_df.loc[mask, 'missing'] = True
    # and copy original
    result_df.loc[mask, 'new_hierarchy'] = result_df.loc[mask, 'original_hierarchy']

    columns = ['original_hierarchy', 'new_hierarchy', 'missing', HIERARCHY_KEY, TAB_DESCRIPTION, LOCATION]
    return result_df[columns]


def apply_name_fixes(df):
    mapper = {
              # otherwise labelled as 'Total | Products | Finished Motor Gasoline | Finished Motor Gasoline Excl. Adjustment
              'W_EPM0F_YPR_NUS_MBBLD': 'Total | Products | Motor Gasoline | Finished Motor Gasoline (Excl. Adjustment)',
              # Net Imports (Including SPR)
              'WCRNTUS2': 'Total | Crude Oil',
              'WRPNTUS2': 'Total | Products',
              'WTTNTUS2': 'Total',
              # Refiner Inputs and Utilization
              'WCRRIUS2': 'Total | Crude Oil',
    }

    # all the (non-crude) products category now needs the following prefix
    mask = (df['missing'].values) & (~df['new_hierarchy'].str.contains('Crude'))
    string = 'Products | '
    df.loc[mask, 'new_hierarchy'] = string + df.loc[mask, 'new_hierarchy']

    # delete the word total from anywhere in the path
    # unless that is the entire new_hierarchy
    string = 'Total'  # no lagging space
    mask = df['new_hierarchy'] != string

    # remove the root node as its copied over in some cases
    # escape the pipe delimiter!
    string = 'Total \| '
    df.loc[mask, 'new_hierarchy'] = df.loc[mask, 'new_hierarchy'].str.replace(string, '')

    # remove from anywhere in the string
    string = 'Total '
    df.loc[mask, 'new_hierarchy'] = df.loc[mask, 'new_hierarchy'].str.replace(string, '')

    # put it back in
    string = 'Total | '
    df.loc[mask, 'new_hierarchy'] = \
        string + df.loc[mask, 'new_hierarchy']

    # if there's no mapping at all then a good default is just Total
    mask = df['new_hierarchy'] == ''
    df.loc[mask, 'new_hierarchy'] = 'Total'

    # remove these strings for consistency
    strings = [r' \(Excluding Ethanol\)', r' \(Including SPR\)']
    for string in strings:
        df['new_hierarchy'] = df['new_hierarchy'].str.replace(string, '')

    # replace 2x spaces with 1x space
    string = '  '
    df['new_hierarchy'] = df['new_hierarchy'].str.replace(string, ' ')

    # fixes based on source key
    for source_key, hierarchy in mapper.items():
        df.at[source_key, 'new_hierarchy'] = hierarchy

    # illegal characters for mapper
    corrections = [(r'/', ' '), (r'(', ''), (r')', '')]
    for pattern, replacement in corrections:
        df['new_hierarchy'] = df['new_hierarchy'].str.replace(pattern, replacement)

    columns = [TAB_DESCRIPTION, 'new_hierarchy']
    df.sort_values(by=columns, axis='index', inplace=True)
    return df


def build_flat_hierarchy_from_list(df):
    # initialise
    df = df.to_frame()
    df['list'] = None
    df['list'] = df['list'].astype('object')

    # parse the full hierarchy name with pipes into a list
    # leading and trailing spaces need to be removed
    df['list'] = df['new_hierarchy'].str.split(pat='|').apply(lambda x: [e.strip() for e in x]).tolist()

    # define new columns
    max_depth = df['list'].apply(len).max()
    columns = numbers_as_words[:max_depth]

    # new df with only the new columns
    hierarchy_df = pd.DataFrame(data=None, index=df.index, columns=columns)
    for i, row in df.iterrows():
        hierarchy_list = row['list']
        hierarchy_df.loc[i] = hierarchy_list + [''] * (max_depth - len(hierarchy_list))
    return hierarchy_df


def build_hierarchy(final_df):
    # prepare for upload to mosaic
    hierarchy_df = final_df['new_hierarchy'].drop_duplicates()
    hierarchy_df.sort_values(axis='index', inplace=True)

    # create the hierarchy as one value per column
    flat_hierarchy_df = build_flat_hierarchy_from_list(hierarchy_df)

    # save to csv for upload to mosaic
    pathfile = os.path.join(path, csv_for_hierarchy_result)
    flat_hierarchy_df.to_csv(pathfile, index=False)


if __name__ == '__main__':
    # get the original and proposed naive hierarchy for each symbol
    # the naive hierarchy is solely described by the manual mapping xls
    analysis_df = build_hierarchy_analysis()

    # some name fixes are applied to the new hierarchy
    final_df = apply_name_fixes(analysis_df)

    # save it because this relates source_key to hierarchy
    # and will be an input to the mapping exercise
    pathfile = os.path.join(path, file_for_mapping_preparation)
    final_df.to_pickle(pathfile)

    # drop dupes and save as csv for upload to mosaic
    build_hierarchy(final_df)
