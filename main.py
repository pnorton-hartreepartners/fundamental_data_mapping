import os
os.environ['IGNITE_HOST_OVERWRITE'] = 'jdbc.dev.mosaic.hartreepartners.com'
os.environ['TSDB_HOST'] = 'tsdb-dev.mosaic.hartreepartners.com'
os.environ['CRATE_HOST'] = 'ttda.cratedb-dev-cluster.mosaic.hartreepartners.com:4200'
os.environ['MOSAIC_ENV'] = 'DEV'

import pandas as pd
from analyst_data_views.common.db_flattener import getFlatRawDF
from constants import path, file_for_mosaic_data, \
    file_for_metadata, SAVE, LOAD
from eia_metadata import get_metadata_df


if __name__ == '__main__':
    data_mode = LOAD

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

    # get metadata and save to file
    exclude_columns = ['updated_at', 'date', 'value']
    columns = [c for c in timeseries_df.columns if c not in exclude_columns]
    metadata_df = get_metadata_df(timeseries_df, columns)
    pathfile = os.path.join(path, file_for_metadata)
    metadata_df.to_pickle(pathfile)
