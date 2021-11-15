path = r'C:\Temp'
# path = r'\\bdcuk\hetco\Data project\Tableau\eia-weekly'

file_for_mosaic_data = 'eia-weekly.pkl'
file_for_timeseries = 'eia_weekly_ts.pkl'
file_for_raw_metadata = 'eia-weekly-metadata-raw.pkl'
file_for_cleaned_metadata = 'eia-weekly-metadata-cleaned.pkl'
file_for_scrape_result = 'eia-weekly-scrape-result.pkl'
file_for_scrape_w_leaf_nodes = 'eia-weekly-scrape-leaf-nodes.pkl'

xlsx_for_cleaned_metadata = 'eia-weekly-metadata-cleaned.xlsx'
xlsx_for_scrape_result = 'eia-weekly-scrape-result.xlsx'
xlsx_scrape_w_leaf_nodes = 'eia-weekly-scrape-leaf-nodes.xlsx'

xlsx_for_seasonality_timeseries = 'eia-weekly-seasonality.xlsx'

xlsx_for_mapping_result = 'eia-weekly-mapping-result.xlsx'  # for mosaic mapper
xlsx_for_timeseries_analysis = 'eia-weekly-timeseries-analysis.xlsx'  # checking hierarchy; not really necessary now

csv_for_timeseries = 'eia_weekly_ts.csv'  # around half-a-million rows so use csv; consumed by tableau chart

SOURCE_KEY = 'Sourcekey'
TAB_DESCRIPTION = 'TabDescription'
LOCATION = 'Location'
UNIT = 'Unit'
DESCRIPTION = 'Description'
LOAD = 'load'
SAVE = 'save'

metadata_reduced_columns = [DESCRIPTION, TAB_DESCRIPTION, LOCATION, UNIT]
terse_timeseries_columns = ['date', SOURCE_KEY, 'value']
