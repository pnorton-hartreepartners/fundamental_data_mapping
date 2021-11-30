import os
import pandas as pd
from constants import path, xlsx_for_manual_hierarchy, file_for_scrape_result, \
    SOURCE_KEY, TAB_DESCRIPTION, LOCATION, file_for_cleaned_metadata

# collect manual mapping from xls
x = pd.ExcelFile(xlsx_for_manual_hierarchy)
sheets_dict = {sheet_name: x.parse(sheet_name) for sheet_name in x.sheet_names if sheet_name.startswith('all')}
raw_mapping_df = pd.concat(sheets_dict.values(), axis='index')

# need the scrape result for the hierarchy name
pathfile = os.path.join(path, file_for_scrape_result)
scrape_result_df = pd.read_pickle(pathfile)

# need the metadata for the table names and locations
pathfile = os.path.join(path, file_for_cleaned_metadata)
metadata_df = pd.read_pickle(pathfile)


# ========
# cleaning
clean_mapping_df = raw_mapping_df.copy(deep=True)

# rename column
HIERARCHY_SOURCE_KEY = 'Hierarchy' + SOURCE_KEY
clean_mapping_df.rename(columns={'Imports': HIERARCHY_SOURCE_KEY}, inplace=True)

# and drop this
clean_mapping_df.drop(TAB_DESCRIPTION, axis='columns', inplace=True)

# we're not interested in rows from the mapping table without an index
mask = pd.isnull(clean_mapping_df[SOURCE_KEY])
clean_mapping_df = clean_mapping_df[~mask]

# but we do care about sourcekeys missing from the imports table
# in this case we'll use the mapping table hierarchy instead
# but we should label that we're doing this
clean_mapping_df['missing'] = pd.isnull(clean_mapping_df[HIERARCHY_SOURCE_KEY])
# now fill missing values and drop the Imports column (which was previously set as index)
clean_mapping_df.loc[:, [HIERARCHY_SOURCE_KEY, SOURCE_KEY]] = \
    clean_mapping_df.loc[:, [HIERARCHY_SOURCE_KEY, SOURCE_KEY]].bfill(axis='columns')

# =====================
# enrich for inspection

result_df1 = pd.merge(clean_mapping_df, scrape_result_df['full_name'], left_on=HIERARCHY_SOURCE_KEY, right_index=True)
result_df1.rename(columns={'full_name': 'new_hierarchy'}, inplace=True)

result_df2 = pd.merge(clean_mapping_df, scrape_result_df['full_name'], left_on=SOURCE_KEY, right_index=True)
result_df2.rename(columns={'full_name': 'original_hierarchy'}, inplace=True)

result_df = pd.concat([result_df1, result_df2], axis='columns')

result_df = pd.merge(result_df, metadata_df[TAB_DESCRIPTION, LOCATION], left_on=SOURCE_KEY, right_index=True)


print()
pass


