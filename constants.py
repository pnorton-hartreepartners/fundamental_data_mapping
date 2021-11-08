path = r'C:\Temp'
file_for_mosaic_data = 'eia-weekly.pkl'
file_for_metadata = 'eia-weekly-metadata.pkl'
file_for_scrape = 'eia-weekly-scrape.pkl '

xlsx_for_timeseries_analysis = 'eia-weekly-timeseries-analysis.xlsx'
xlsx_for_mapping_result = 'eia-weekly-mapping-result.xlsx'
xlsx_for_scrape_result = 'eia-weekly-scrape-result.xlsx'
xlsx_for_enriched_metadata = 'eia-weekly-metadata-enriched.xlsx'

SOURCE_KEY = 'Sourcekey'
TAB_DESCRIPTION = 'TabDescription'
LOCATION = 'Location'
UNIT = 'Unit'
DESCRIPTION = 'Description'
LOAD = 'load'
SAVE = 'save'

metadata_reduced_columns = [DESCRIPTION, TAB_DESCRIPTION, LOCATION, UNIT]
scrape_data_reduced_columns = ['text', 'level', 'year_start', 'year_end', 'level_change']

