'''
functions for building and using metadata
also we modify text to fix source data typos
this is NOT part of the mosaic mapping exercise
'''

import os
import pandas as pd
from constants import path, file_for_raw_metadata, SOURCE_KEY, DESCRIPTION, LOCATION, xlsx_for_cleaned_metadata, \
    file_for_cleaned_metadata


def get_all_metadata_for_symbol(metadata_df, source_key):
    return list(metadata_df.loc[metadata_df[SOURCE_KEY] == source_key].values[0])


def get_single_metadata_dict_for_all_symbols(metadata_df, label):
    return dict(zip(metadata_df.index, metadata_df[label].values))


def get_metadata_df(df, columns):
    metadata_df = df[columns].drop_duplicates()
    metadata_df.set_index(SOURCE_KEY, drop=True, inplace=True)
    return metadata_df


def clean_description_metadata_df(df):
    list_of_replacements = [('Ending Stocks of ', ''), ('Ending Stocks', ''),
                            ('Exports of ', ''), ('Exports', ''),
                            ('Imports of ', ''), ('Imports from ', ''), ('Imports', ''),
                            ('Net Production of ', ''), ('Net Production', ''),
                            ('Net Input of ', ''), ('Net Input', ''),
                            ('Days of Supply of ', ''),
                            ('Product Supplied of ', ''),
                            ('Refiner and Blender', ''), ('Refiner Blender and ', ''),
                            (' of Crude Oil', '')
                            ]

    df['name'] = df[DESCRIPTION]
    for replacement in list_of_replacements:
        df['name'] = df['name'].replace(*replacement)

    df['name'].str.lstrip(' ')
    df['name'].str.capitalize()


def clean_location_metadata_df(df):
    list_of_replacements = [{'Rocky Mountains (PADD 4)': 'Rocky Mountain (PADD 4)'},
                            {'Midwest (PADD2)': 'Midwest (PADD 2)'},
                            ]

    for replacement in list_of_replacements:
        df[LOCATION] = df[LOCATION].replace(*replacement)


def build_clean_metadata():
    # load the raw metadata
    pathfile = os.path.join(path, file_for_raw_metadata)
    metadata_df = pd.read_pickle(pathfile)

    # clean it
    clean_description_metadata_df(metadata_df)
    clean_location_metadata_df(metadata_df)

    # save it
    pathfile = os.path.join(path, file_for_cleaned_metadata)
    metadata_df.to_pickle(pathfile)

    pathfile = os.path.join(path, xlsx_for_cleaned_metadata)
    with pd.ExcelWriter(pathfile) as writer:
        metadata_df.to_excel(writer, sheet_name='metadata')


if __name__ == '__main__':
    build_clean_metadata()

