import os
import pandas as pd
from constants import path, csv_for_timeseries, xlsx_for_seasonality_timeseries


def build_calyear_weekly_seasonality(dates):
    years = dates.groupby(dates.year)
    # dictionary of year and max date; except current year ends early obviously
    end_of_years = {y: years[y].max() for y in years.keys()}

    def incremental_dates_for_year(last_date):
        # extend with lots of weeks
        incremental_dates = pd.date_range(last_date, freq="7D", periods=53)
        # trim to the current year
        return incremental_dates[incremental_dates.year == last_date.year]

    # get max date for current year and update dict
    this_year = dates.max().year
    incremental_dates = incremental_dates_for_year(dates.max())
    end_of_years[this_year] = incremental_dates.max()

    # extend this year so we don't have to run script every week
    dates = dates.union(incremental_dates)
    index = pd.Index(dates, name='date')

    df = pd.DataFrame(index=index)
    for i, row in df.iterrows():
        end_of_year = end_of_years[i.year]
        # returns a negative difference so charting works in the right direction
        days = pd.to_datetime(i) - pd.to_datetime(end_of_year)
        weeks = int(pd.Timedelta(days).days / 7)
        df.loc[i, 'year'] = i.year
        df.loc[i, 'weeks_from_expiry'] = weeks
        df.loc[i, 'date_of_this_year'] = end_of_years[this_year] + pd.tseries.offsets.Week(weeks)

    return df


def build_seasonality_ts():
    # load terse timeseries data extracted from db
    pathfile = os.path.join(path, csv_for_timeseries)
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


if __name__ == '__main__':
    build_seasonality_ts()



