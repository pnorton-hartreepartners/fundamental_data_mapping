'''
this script builds all the data for the tableau workbook
that presents all the weekly eia data
it includes a scrape of the webpage that defines the hierarchy
'''

import os

os.environ['IGNITE_HOST_OVERWRITE'] = 'jdbc.dev.mosaic.hartreepartners.com'
os.environ['TSDB_HOST'] = 'tsdb-dev.mosaic.hartreepartners.com'
os.environ['CRATE_HOST'] = 'ttda.cratedb-dev-cluster.mosaic.hartreepartners.com:4200'
os.environ['MOSAIC_ENV'] = 'DEV'

import pandas as pd
import argparse
from analyst_data_views.common.db_flattener import getFlatRawDF
from constants import path, file_for_mosaic_data, \
    file_for_raw_metadata, SAVE, LOAD, REFRESH, csv_for_timeseries, \
    terse_timeseries_columns, file_for_timeseries, xlsx_for_timeseries
from eia_seasonality_dates import build_seasonality_ts
from eia_metadata import _get_metadata_df, build_clean_metadata, build_raw_metadata
from eia_scrape import build_all_scrape
from eia_trees import build_all_tree_analysis


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mode',
                        help='save, load or refresh',
                        choices=[SAVE, LOAD, REFRESH],
                        required=True)
    return parser.parse_args()


def get_full_timeseries(data_mode):
    if data_mode in [SAVE, REFRESH]:
        timeseries_df = getFlatRawDF(source='eia-weekly')
        pathfile = os.path.join(path, file_for_mosaic_data)
        timeseries_df.to_pickle(pathfile)
    elif data_mode == LOAD:
        pathfile = os.path.join(path, file_for_mosaic_data)
        timeseries_df = pd.read_pickle(pathfile)
    else:
        raise NotImplementedError
    return timeseries_df





if __name__ == '__main__':
    '''
    python zzz_main.py --mode load
    # refresh means... just refresh the timeseries when new data is published
    # load means... load the timeseries from the saved pickle
    # save means... build and save everything
    '''
    args = get_args()

    # =============================================================================
    # create terse timeseries ie sourcekey, date and value only
    timeseries_df = get_full_timeseries(args.mode)

    # save as csv for tableau... and pickle for everything else
    pathfile = os.path.join(path, file_for_timeseries)
    timeseries_df[terse_timeseries_columns].to_pickle(pathfile)

    pathfile = os.path.join(path, csv_for_timeseries)
    timeseries_df[terse_timeseries_columns].to_csv(pathfile, index=False)

    pathfile = os.path.join(path, xlsx_for_timeseries)
    with pd.ExcelWriter(pathfile) as writer:
        timeseries_df[terse_timeseries_columns].to_excel(writer, sheet_name='timeseries')

    # build seasonality dates... run every week in case of changes to reporting dates
    build_seasonality_ts()

    if args.mode == SAVE:
        # build metadata and save to file
        build_raw_metadata()

        # clean names and locations
        build_clean_metadata()

        # run webscrape, build metadata and save to file
        build_all_scrape()

        # identify leaf nodes
        build_all_tree_analysis()
