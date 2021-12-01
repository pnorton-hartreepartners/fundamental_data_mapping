import os
import pandas as pd
from constants import path, xlsx_for_manual_hierarchy, file_for_scrape_result, \
    SOURCE_KEY, TAB_DESCRIPTION, LOCATION, file_for_cleaned_metadata, numbers_as_words, csv_for_hierarchy_result, \
    file_for_mapping_preparation


def build_hierarchy(final_df):
    # prepare for upload to mosaic
    hierarchy_df = final_df['new_hierarchy'].drop_duplicates()
    hierarchy_df.sort_values(axis='index', inplace=True)

    # create the hierarchy as one value per column
    flat_hierarchy_df = build_flat_hierarchy_from_list(hierarchy_df)

    # save to csv for upload to mosaic
    pathfile = os.path.join(path, csv_for_hierarchy_result)
    flat_hierarchy_df.to_csv(pathfile, index=False)


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

    # but we do care about sourcekeys missing from the imports table
    # in this case we'll use the original hierarchy instead
    # but we should label that we're doing this
    clean_mapping_df['missing'] = pd.isnull(clean_mapping_df[HIERARCHY_KEY])
    # now fill missing values
    clean_mapping_df.loc[:, [HIERARCHY_KEY, SOURCE_KEY]] = \
        clean_mapping_df.loc[:, [HIERARCHY_KEY, SOURCE_KEY]].bfill(axis='columns')

    # ================================================================
    # get both the original and the proposed hierarchy side-by-side for visual inspection

    # add the new hierarchy
    result_df1 = pd.merge(clean_mapping_df, scrape_result_df['full_name'],
                          left_on=HIERARCHY_KEY, right_index=True)
    result_df1.rename(columns={'full_name': 'new_hierarchy'}, inplace=True)
    result_df1.set_index(SOURCE_KEY, drop=True, inplace=True)

    # and the hierarchy from the mapped symbol
    result_df2 = pd.merge(clean_mapping_df, scrape_result_df['full_name'],
                          left_on=SOURCE_KEY, right_index=True)
    result_df2.rename(columns={'full_name': 'original_hierarchy'}, inplace=True)
    result_df2.set_index(SOURCE_KEY, drop=True, inplace=True)

    # join the two resultsets
    result_df = pd.merge(result_df1['new_hierarchy'], result_df2, left_index=True, right_index=True)

    # add some metadata
    result_df = pd.merge(result_df, metadata_df[[TAB_DESCRIPTION, LOCATION]],
                         left_on=SOURCE_KEY, right_index=True)
    columns = ['original_hierarchy', 'new_hierarchy', 'missing', HIERARCHY_KEY, TAB_DESCRIPTION, LOCATION]
    return result_df[columns]


def apply_name_fixes(df):
    mapper = {'W_EPM0F_YPR_NUS_MBBLD': 'Total | Products | Motor Gasoline | Finished Motor Gasoline (Excl. Adjustment)'}

    # all the (non-crude) products category now needs the following prefix
    mask = (df['missing'].values) & (~df['new_hierarchy'].str.contains('Crude'))
    string = 'Total Products | '
    df.loc[mask, 'new_hierarchy'] = string + df.loc[mask, 'new_hierarchy']

    # this word is often contained along the branch and is superfluous
    string = 'Total '
    df['new_hierarchy'] = df['new_hierarchy'].str.replace(string, '')

    # but in some reports there is a specific root level Total
    # so add this level with a delimiter
    mask = df['new_hierarchy'] == 'Total'
    df.loc[~mask, 'new_hierarchy'] = 'Total | ' + df.loc[~mask, 'new_hierarchy']

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


if __name__ == '__main__':
    # get the original and proposed naive hierarchy for each symbol
    # the naive hierarchy is solely described by the manual mapping xls
    analysis_df = build_hierarchy_analysis()

    # some name fixes are applied to the new hierarchy
    final_df = apply_name_fixes(analysis_df)

    # save it because now we have to extend to all locations
    pathfile = os.path.join(path, file_for_mapping_preparation)
    final_df.to_pickle(pathfile)

    # drop dupes and save as csv for upload to mosaic
    build_hierarchy(final_df)
