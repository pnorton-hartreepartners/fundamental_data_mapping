path = r'C:\Temp'
file_for_mosaic_data = 'eia-weekly.pkl'
file_for_metadata = 'eia-weekly-metadata.pkl'
file_for_scrape = 'eia-weekly-scrape.pkl '

xlsx_for_timeseries_analysis = 'eia-weekly-timeseries-analysis.xlsx'
xlsx_for_mapping_result = 'eia-weekly-mapping-result.xlsx'
xlsx_for_scrape_result = 'eia-weekly-scrape-result.xlsx'
xlsx_for_cleaned_metadata = 'eia-weekly-metadata-cleaned.xlsx'

csv_for_tableau_timeseries = 'eia_weekly_ts.csv'
xlsx_for_seasonality_timeseries = 'eia_weekly_seasonality.xlsx'

SOURCE_KEY = 'Sourcekey'
TAB_DESCRIPTION = 'TabDescription'
LOCATION = 'Location'
UNIT = 'Unit'
DESCRIPTION = 'Description'
LOAD = 'load'
SAVE = 'save'

metadata_reduced_columns = [DESCRIPTION, TAB_DESCRIPTION, LOCATION, UNIT]
scrape_data_reduced_columns = ['text', 'level', 'year_start', 'year_end', 'level_change', 'full_name']
terse_timeseries_columns = ['date', SOURCE_KEY, 'value']
