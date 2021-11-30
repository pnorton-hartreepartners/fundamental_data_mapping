import os
import pandas as pd
from constants import path, xlsx_for_manual_hierarchy, file_for_scrape_result, SOURCE_KEY, TAB_DESCRIPTION

# collect manual mapping from xls
x = pd.ExcelFile(xlsx_for_manual_hierarchy)
sheets_dict = {sheet_name: x.parse(sheet_name) for sheet_name in x.sheet_names if sheet_name.startswith('all')}
raw_mapping_df = pd.concat(sheets_dict.values(), axis='index')


# imports may not have a matching hierarchy, in which case fill nans using the key from the alternative table
clean_mapping_df = raw_mapping_df[['Imports', SOURCE_KEY]].bfill(axis='columns')
# add table description back in
clean_mapping_df = pd.concat([clean_mapping_df, raw_mapping_df[TAB_DESCRIPTION]], axis='columns')
# ffill the table description
clean_mapping_df[TAB_DESCRIPTION].ffill(axis='index', inplace=True)
# we're mapping the source_key from the imports table so we can collect the hierarchy name
clean_mapping_df['index'] = clean_mapping_df.index  # the previous index is retained for ordering
clean_mapping_df.set_index('Imports', drop=True, inplace=True)  # were going to join on this
clean_mapping_df.index.name = SOURCE_KEY

pathfile = os.path.join(path, file_for_scrape_result)
scrape_result_df = pd.read_pickle(pathfile)

scrape_excl_columns = ['level', 'year_start', 'year_end', 'level_change']
scrape_front_columns = ['text', 'symbol_list', 'full_name']
result_df = pd.merge(scrape_result_df, clean_mapping_df, how='outer', left_index=True, right_index=True)

# exclude rows that dont require mapping
mask = pd.isnull(result_df[SOURCE_KEY])
result_df.loc[mask, [SOURCE_KEY, TAB_DESCRIPTION, 'full_name']]



print()
pass


