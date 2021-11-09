import pandas as pd


def build_calyear_weekly_seasonality(dates):
    years = dates.groupby(dates.year)
    # dictionary of year and max date; except current year ends early obviously
    end_of_years = {y: years[y].max() for y in years.keys()}

    def end_of_year(last_date):
        # extend with lots of weeks
        incremental_dates = pd.date_range(last_date, freq="W", periods=53)
        # return the maximum week date in this year
        return incremental_dates[incremental_dates.year == last_date.year].max()

    # get max date for current year and update dict
    this_year = dates.max().year
    end_of_years[this_year] = end_of_year(dates.max())

    df = pd.DataFrame(index=dates)
    for i, row in df.iterrows():
        end_of_year = end_of_years[i.year]
        # returns a negative difference so charting works in the right direction
        days = pd.to_datetime(i) - pd.to_datetime(end_of_year)
        weeks = int(pd.Timedelta(days).days / 7)
        df.loc[i, 'year'] = i.year
        df.loc[i, 'weeks_from_expiry'] = weeks
        df.loc[i, 'date_of_this_year'] = end_of_years[this_year] + pd.tseries.offsets.Week(weeks)

    return df



