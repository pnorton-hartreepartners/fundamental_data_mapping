'''
functions for building and using metadata
also we modify text to fix source data typos
this is NOT part of the mosaic mapping exercise
'''

import os
import pandas as pd
from constants import path, file_for_raw_metadata, SOURCE_KEY, DESCRIPTION, LOCATION, xlsx_for_cleaned_metadata, \
    file_for_cleaned_metadata, TAB_DESCRIPTION, PRODUCT_CODE


def metadata_health_check1(df):
    # confirm there is only one entry for every combination of these columns
    x = df.groupby([TAB_DESCRIPTION, DESCRIPTION, LOCATION]).size()
    assert x[x > 1].empty


def metadata_health_check2(df):
    # check integrity of description and product code
    x_columns = [TAB_DESCRIPTION, DESCRIPTION, LOCATION]
    y_columns = [TAB_DESCRIPTION, PRODUCT_CODE, LOCATION]

    # if these combined keys are a primary key then there will be one source key per group
    # but theyre not
    x = df.groupby(x_columns)
    y = df.groupby(y_columns)

    # where the combined key does not define a unique row
    x_count = x.size()[x.size() > 1]
    y_count = y.size()[y.size() > 1]

    # so combined key --> sourcekey is a one-to-many relationship
    # therefore we turn the combined key into an index
    index_x = [tuple(xx) for xx in x.groups.keys()]
    index_y = [tuple(yy) for yy in y.groups.keys()]

    # and the sourcekey is the data
    x_df = pd.DataFrame(data=x.groups.values(), index=pd.MultiIndex.from_tuples(index_x, names=x_columns), columns=[SOURCE_KEY])
    y_df = pd.DataFrame(data=y.groups.values(), index=pd.MultiIndex.from_tuples(index_y, names=y_columns), columns=[SOURCE_KEY])

    compare_df = pd.merge(x_df, y_df, how='outer', left_index=True, right_index=True)

    mask1 = compare_df[TAB_DESCRIPTION + '_x'] != compare_df[TAB_DESCRIPTION + '_y']
    mask2 = compare_df[LOCATION + '_x'] != compare_df[LOCATION + '_y']

    result_df = compare_df[(mask1 | mask2)]
    result_df.to_clipboard()


    assert x == y


def get_metadata_df(df, columns):
    metadata_df = df[columns].drop_duplicates()
    metadata_df.set_index(SOURCE_KEY, drop=True, inplace=True)
    return metadata_df


def clean_location_metadata_df(df):
    df['raw_location'] = df[LOCATION]
    list_of_replacements = [{'Rocky Mountains (PADD 4)': 'Rocky Mountain (PADD 4)'},
                            {'Midwest (PADD2)': 'Midwest (PADD 2)'},
                            ]

    for replacement in list_of_replacements:
        df[LOCATION] = df[LOCATION].replace(replacement)


def clean_product_code_metadata_df(df):
    df['raw_product_code'] = df[PRODUCT_CODE]
    list_of_replacements = [('WRPIMUS2', 'WTX'),
                            ]

    for key, value in list_of_replacements:
        df.loc[key, PRODUCT_CODE] = value


def build_clean_metadata():
    # load the raw metadata
    pathfile = os.path.join(path, file_for_raw_metadata)
    metadata_df = pd.read_pickle(pathfile)

    # clean it
    clean_location_metadata_df(metadata_df)
    clean_product_code_metadata_df(metadata_df)

    # check it
    metadata_health_check1(metadata_df)
    metadata_health_check2(metadata_df)

    # save it
    pathfile = os.path.join(path, file_for_cleaned_metadata)
    metadata_df.to_pickle(pathfile)

    pathfile = os.path.join(path, xlsx_for_cleaned_metadata)
    with pd.ExcelWriter(pathfile) as writer:
        metadata_df.to_excel(writer, sheet_name='metadata')


if __name__ == '__main__':
    build_clean_metadata()

