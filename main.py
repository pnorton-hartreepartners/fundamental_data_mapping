import os

from eia_scrape import build_all_scrape

os.environ['IGNITE_HOST_OVERWRITE'] = 'jdbc.dev.mosaic.hartreepartners.com'
os.environ['TSDB_HOST'] = 'tsdb-dev.mosaic.hartreepartners.com'
os.environ['CRATE_HOST'] = 'ttda.cratedb-dev-cluster.mosaic.hartreepartners.com:4200'
os.environ['MOSAIC_ENV'] = 'DEV'

import pandas as pd
from analyst_data_views.common.db_flattener import getFlatRawDF
from constants import path, file_for_mosaic_data, \
    file_for_metadata, SAVE, LOAD, csv_for_tableau_timeseries, xlsx_for_seasonality_timeseries, \
    terse_timeseries_columns, file_for_scrape, xlsx_for_scrape_result
from eia_seasonality_dates import build_calyear_weekly_seasonality
from eia_metadata import get_metadata_df


if __name__ == '__main__':
    data_mode = SAVE

    if data_mode == SAVE:
        # symbol, date, value format
        timeseries_df = getFlatRawDF(source='eia-weekly')
        pathfile = os.path.join(path, file_for_mosaic_data)
        timeseries_df.to_pickle(pathfile)
    elif data_mode == LOAD:
        pathfile = os.path.join(path, file_for_mosaic_data)
        timeseries_df = pd.read_pickle(pathfile)
    else:
        raise NotImplementedError

    # =============================================================================
    # create terse timeseries as csv for tableau

    pathfile = os.path.join(path, csv_for_tableau_timeseries)
    timeseries_df[terse_timeseries_columns].to_csv(pathfile, index=False)

    # =============================================================================
    # build metadata and save to file

    exclude_columns = ['updated_at', 'date', 'value']
    columns = [c for c in timeseries_df.columns if c not in exclude_columns]
    metadata_df = get_metadata_df(timeseries_df, columns)

    pathfile = os.path.join(path, file_for_metadata)
    metadata_df.to_pickle(pathfile)

    # =============================================================================
    # run webscrape, build metadata and save to file
    build_all_scrape(file_for_metadata, file_for_scrape, xlsx_for_scrape_result)

    # =============================================================================
    # build seasonality dates

    # load terse timeseries data extracted from db
    pathfile = os.path.join(path, csv_for_tableau_timeseries)
    timeseries_df = pd.read_csv(pathfile)

    # create a clean date index
    dates_df = timeseries_df['date'].drop_duplicates()
    dates_index = pd.to_datetime(dates_df.values)
    dates_index.name = 'date'

    # calculate a time to expiry
    seasonality_df = build_calyear_weekly_seasonality(dates_index)

    # save as xls
    pathfile = os.path.join(path, xlsx_for_seasonality_timeseries)
    with pd.ExcelWriter(pathfile) as writer:
        seasonality_df.to_excel(writer, sheet_name='seasonality')
