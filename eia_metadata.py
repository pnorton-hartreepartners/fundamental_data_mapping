'''
functions for building and using metadata
also we modify text to fix source data typos
this is NOT part of the mosaic mapping exercise
'''

import os
import pandas as pd
from constants import path, file_for_raw_metadata, SOURCE_KEY, DESCRIPTION, LOCATION, xlsx_for_cleaned_metadata, \
    file_for_cleaned_metadata, file_for_mosaic_data
from eia_healthcheck_metadata import metadata_health_check1, metadata_health_check2, metadata_health_check3

# ======================================================
# config for metadata construction

# drop duplicates after removing these columns
exclude_columns = ['Sheet', 'updated_at', 'date', 'value']
# only use timeseries since this date to exclude historic nonsense
after_date = pd.to_datetime('2020-01-01')

location_corrections = [{'Rocky Mountains (PADD 4)': 'Rocky Mountain (PADD 4)'},
                        {'Midwest (PADD2)': 'Midwest (PADD 2)'},
                        ]

description_corrections = [
    ('W_EPOBGRR_IM0_R40-Z00_MBBLD', 'Imports from All Countries of Motor Gasoline Blending Components RBOB'),
    ('W_EPM0CAL55_IM0_R50-Z00_MBBLD', 'Imports of Motor Gasoline Finished Conventional Ed55 and Lower'),
    ('WO9IM_R40-Z00_2', 'Imports of Conventional Other Gasoline Blending Components'),
    ('WG3ST_R40_1', 'Ending Stocks of Reformulated Motor Gasoline Non-Oxygentated'),
    ('WD1TP_NUS_2', 'Refiner and Blender Net Production of Distillate Fuel Oil Greater than 15 to 500 ppm Sulfur')
    ]
# ======================================================


def build_raw_metadata():
    # get the timeseries data
    pathfile = os.path.join(path, file_for_mosaic_data)
    timeseries_df = pd.read_pickle(pathfile)
    # create the raw metadata
    metadata_df = _get_metadata_df(timeseries_df)
    # save it
    pathfile = os.path.join(path, file_for_raw_metadata)
    metadata_df.to_pickle(pathfile)


def _get_metadata_df(df):
    mask = df['date'] >= after_date
    columns = [c for c in df.columns if c not in exclude_columns]
    metadata_df = df.loc[mask, columns].drop_duplicates()
    metadata_df.set_index(SOURCE_KEY, drop=True, inplace=True)
    return metadata_df


def clean_location_metadata_df(df):
    df['raw_location'] = df[LOCATION]
    for replacement in location_corrections:
        df[LOCATION] = df[LOCATION].replace(replacement)


def clean_description_metadata_df(df):
    # change the description because we use this as a join field
    # when expanding mapping across locations
    df['raw_description'] = df[DESCRIPTION]
    for key, value in description_corrections:
        df.loc[key, DESCRIPTION] = value


def build_clean_metadata():
    # load the raw metadata
    pathfile = os.path.join(path, file_for_raw_metadata)
    metadata_df = pd.read_pickle(pathfile)

    # clean it
    clean_location_metadata_df(metadata_df)
    clean_description_metadata_df(metadata_df)

    # save it
    pathfile = os.path.join(path, file_for_cleaned_metadata)
    metadata_df.to_pickle(pathfile)

    pathfile = os.path.join(path, xlsx_for_cleaned_metadata)
    with pd.ExcelWriter(pathfile) as writer:
        metadata_df.to_excel(writer, sheet_name='metadata')


if __name__ == '__main__':
    # build metadata and save to file
    build_raw_metadata()

    # clean descriptions and locations
    build_clean_metadata()

    # do some healthchecks
    pathfile = os.path.join(path, file_for_cleaned_metadata)
    metadata_df = pd.read_pickle(pathfile)

    metadata_health_check1(metadata_df)
    metadata_health_check2(metadata_df)
    metadata_health_check3(metadata_df)


