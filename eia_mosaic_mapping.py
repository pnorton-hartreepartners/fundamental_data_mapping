import os
import numpy as np
import pandas as pd
from constants import path, csv_for_hierarchy_result, xlsx_for_mapping_result, SOURCE_KEY, numbers_as_words, \
    file_for_scrape_result, xlsx_for_mapping_errors
from eia_scrape import leaf, branch


def get_mapping_df(df):
    # build a df with both leaf and full branch syntax
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


def build_all_mapping():
    # get the saved scrape result
    pathfile = os.path.join(path, file_for_scrape_result)
    report_df = pd.read_pickle(pathfile)

    # select flat hierarchy from RHS of df
    first_column_index = report_df.columns.get_loc('zero')
    report_df = report_df[report_df.columns[first_column_index:]]

    # create hierarchy
    hierarchy_df = report_df.drop_duplicates(ignore_index=True)
    # create mapping with both leaf and branch syntax
    # naive because theres no awareness of upload errors at this stage
    naive_mapping_df = get_mapping_df(report_df)

    # =================================================================
    # there will be upload errors; this is an iterative approach to fixing

    # collect all the errors; different sheets have different lists
    # keep this xls in source control; just in case
    x = pd.ExcelFile(xlsx_for_mapping_errors)
    errors_dict = {sheet_name: x.parse(sheet_name) for sheet_name in x.sheet_names}

    # ===================
    # first set of errors
    errors_df = errors_dict['one']
    error_keys = errors_df[SOURCE_KEY].values

    # assume errors are symbols that require the branch syntax
    mask = naive_mapping_df.index.isin(error_keys)
    branch_df = naive_mapping_df.loc[mask, branch]
    leaf_df = naive_mapping_df.loc[~mask, leaf]
    final_mapping_df = pd.concat([leaf_df, branch_df], axis='index')

    # ====================
    # second set of errors
    errors_df = errors_dict['two']
    error_keys = errors_df[SOURCE_KEY].values

    # more butchery; remove the zeroth node for 'Total Products'
    mask = final_mapping_df.index.isin(error_keys)
    texts = final_mapping_df[mask].str.split(':').values
    new_texts = []
    for prefix, text in texts:
        if text.startswith('Total Products@'):
            i = numbers_as_words.index(prefix)
            new_prefix = numbers_as_words[i - 1]
            new_text = text.replace('Total Products@', '')
            new_texts.append([new_prefix, new_text])
        else:
            new_texts.append([prefix, text])
    data = [':'.join(text) for text in new_texts]
    index = final_mapping_df[mask].index
    total_products_df = pd.DataFrame(data=data, index=index)
    final_mapping_df = pd.concat([final_mapping_df[~mask], total_products_df], axis='index')

    # =================================================================
    # third set of errors; we didnt get this far
    errors_df = errors_dict['three']
    error_keys = errors_df[SOURCE_KEY].values

    # =================================================================
    # save the mapping
    pathfile = os.path.join(path, xlsx_for_mapping_result)
    with pd.ExcelWriter(pathfile) as writer:
        final_mapping_df.to_excel(writer, sheet_name='mapping')

    # save the hierarchy; smash the patriarchy
    pathfile = os.path.join(path, csv_for_hierarchy_result)
    hierarchy_df.to_csv(pathfile, index=False)


if __name__ == '__main__':
    build_all_mapping()

