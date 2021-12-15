path = r'C:\Temp'
# path = r'\\bdcuk\hetco\Data project\Tableau\eia-weekly'

file_for_mosaic_data = 'eia-weekly.pkl'
file_for_timeseries = 'eia-weekly-ts.pkl'
file_for_raw_metadata = 'eia-weekly-metadata-raw.pkl'
file_for_mapping_preparation = 'eia-weekly-mapping-preparation.pkl'

file_for_cleaned_metadata = 'eia-weekly-metadata-cleaned.pkl'
file_for_scrape_result = 'eia-weekly-scrape-result.pkl'
file_for_scrape_w_leaf_nodes = 'eia-weekly-scrape-leaf-nodes.pkl'

xlsx_for_cleaned_metadata = 'eia-weekly-metadata-cleaned.xlsx'
xlsx_for_scrape_result = 'eia-weekly-scrape-result.xlsx'
xlsx_scrape_w_leaf_nodes = 'eia-weekly-scrape-leaf-nodes.xlsx'

xlsx_for_mapping_errors = 'eia-weekly-xlsx... mapping errors.xlsx'
xlsx_for_manual_hierarchy = 'eia-weekly-xlsx... manual hierarchy.xlsx'

xlsx_for_seasonality_timeseries = 'eia-weekly-seasonality.xlsx'

csv_for_hierarchy_result = 'eia-weekly-hierarchy-result.csv'
xlsx_for_map_product_result = 'eia-weekly-map-product-result.xlsx'
xlsx_for_map_remaining_result = 'eia-weekly-map-remaining-result.xlsx'

csv_for_timeseries = 'eia-weekly-ts.csv'  # around half-a-million rows so use csv; consumed by tableau chart
xlsx_for_timeseries = 'eia-weekly-ts.xlsx'  # although tableau driver pukes so use xls instead

xlsx_for_timeseries_analysis = 'eia-weekly-timeseries-analysis.xlsx'  # checking hierarchy; not really necessary now

SOURCE_KEY = 'Sourcekey'
DESCRIPTION = 'Description'
PRODUCT_CODE = 'ProductCode'
TAB_DESCRIPTION = 'TabDescription'
UNIT = 'Unit'
LOCATION = 'Location'

LOAD = 'load'
SAVE = 'save'
REFRESH = 'refresh'

metadata_reduced_columns = [DESCRIPTION, TAB_DESCRIPTION, LOCATION, UNIT]
metadata_enrich_columns = [TAB_DESCRIPTION, LOCATION]
terse_timeseries_columns = ['date', SOURCE_KEY, 'value']

numbers_as_words = ['zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten']
