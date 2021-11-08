import os
import pandas as pd
from constants import path, csv_for_tableau_timeseries, xlsx_for_seasonality_timeseries


def build_calyear_weekly_seasonality(dates):
    years = dates.groupby(dates.year)
    end_of_years = {y: years[y].max() for y in years.keys()}

    def end_of_year(last_date):
        last_cal_day_of_this_year = pd.to_datetime(f'{last_date.year}-12-31')
        incremental_dates = pd.date_range(last_date, freq="W", periods=52)
        return incremental_dates[incremental_dates <= last_cal_day_of_this_year].max()

    # get max date for current year and update dict
    this_year = dates.max().year
    end_of_years[this_year] = end_of_year(dates.max())

    df = pd.DataFrame(index=dates)
    for i, row in df.iterrows():
        end_of_year = end_of_years[i.year]
        days = pd.to_datetime(end_of_year) - pd.to_datetime(i)
        week = int(pd.Timedelta(days).days / 7)
        df.loc[i, 'year'] = i.year
        df.loc[i, 'weeks_from_expiry'] = week

    return df


if __name__ == '__main__':
    # load timeseries data
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
