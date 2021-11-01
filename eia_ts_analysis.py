'''
functions to select data based on dict of hierarchy of symbols
 - test that the sum of the children equates to the parent
 - generate a pivotted df to chart the children and parent
output is a pivotted df in an xls for human consumption
'''

import os
import pandas as pd
import datetime as dt
from eia_hierarchy_definitions import hierarchy_dict_us_stocks
from eia_metadata import get_single_metadata_dict_for_all_symbols
from constants import path, file_for_mosaic_data, xlsx_for_timeseries_analysis, \
    file_for_metadata, SOURCE_KEY, DESCRIPTION

SUBTOTAL = 'subtotal'
CALCULATED = 'calculated'
DATE = 'date'
VALUE = 'value'


def build_comparison(df, source_key, source_keys):
    # subtotal
    sub_total_df = get_subtotal_df(df, source_key)
    sub_total_df.columns = [SUBTOTAL]

    # calculation
    calc_total_df = get_components_df(df, source_keys)
    calc_total_df = get_components_sum_df(calc_total_df)
    calc_total_df.columns = [CALCULATED]

    # join, calculation difference and clean
    report_df = sub_total_df.join(calc_total_df)
    report_df['diff'] = report_df[SUBTOTAL] - report_df[CALCULATED]
    report_df.dropna(axis='index', how='any', inplace=True)
    report_df.sort_index(axis='index', inplace=True)

    return report_df


def get_components_df(df, source_keys):
    mask = df[SOURCE_KEY].isin(source_keys)
    return df[mask]


def get_components_sum_df(df):
    return df.groupby(DATE).sum()


def get_components_pivot_df(df):
    df = df.pivot(columns=SOURCE_KEY)
    df.columns = df.columns.droplevel(0)
    return df


def get_subtotal_df(df, source_key):
    mask = df[SOURCE_KEY] == source_key
    sub_total_df = df.loc[mask, [DATE, VALUE]]
    sub_total_df.set_index(DATE, drop=True, inplace=True)
    sub_total_df.columns = [source_key]
    return sub_total_df


if __name__ == '__main__':
    source_key = 'WTTSTUS1'

    # load timeseries data
    pathfile = os.path.join(path, file_for_mosaic_data)
    timeseries_df = pd.read_pickle(pathfile)

    # load metadata
    pathfile = os.path.join(path, file_for_metadata)
    metadata_df = pd.read_pickle(pathfile)

    # =================================================================
    # do some analysis; compare the total with the sum of the parts

    # select which hierarchy mapping to look at and build comparison to calc diffs
    selection = hierarchy_dict_us_stocks[source_key]
    comparison_df = build_comparison(timeseries_df, source_key=source_key, source_keys=selection)

    # =================================================================
    # do some more analysis; report the total and the parts as a pivot

    # collect timeseries for all the component symbols and pivot
    df_norm = get_components_df(timeseries_df, selection)
    df_norm.set_index(DATE, drop=True, inplace=True)
    df_norm = df_norm[[SOURCE_KEY, VALUE]]
    df_pivot = get_components_pivot_df(df_norm)

    # get total
    df_total = get_subtotal_df(timeseries_df, source_key)

    # append the total to the pivot and sort
    df_combo = pd.concat([df_pivot, df_total], axis='columns')
    df_combo.sort_index(inplace=True)

    # rename columns on pivot
    # select just one field here and get a dict mapping source_key to label
    metadata_dict = get_single_metadata_dict_for_all_symbols(metadata_df, label=DESCRIPTION)
    mapper = {k: v.replace('Ending Stocks of ', '') for k, v in metadata_dict.items()}
    mapper = {k: v.replace('Ending Stocks', '') for k, v in mapper.items()}
    df_combo.rename(columns=mapper, inplace=True)

    # filter to remove dodgy history
    mask = df_combo.index > dt.date(2015, 1, 1)
    df_combo = df_combo[mask]

    # save as xls
    chart_title = metadata_dict[source_key]
    pathfile = os.path.join(path, xlsx_for_timeseries_analysis)
    with pd.ExcelWriter(pathfile) as writer:
        df_norm.to_excel(writer, sheet_name='df_norm')
        df_combo.to_excel(writer, sheet_name='df_combo')
