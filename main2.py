'''
this script builds the hierarchy data for eia products
and all the mapping
'''

import os
import pandas as pd
from constants import path, file_for_mapping_preparation, file_for_raw_metadata, xlsx_for_map_remaining_result
from eia_hierarchy import build_hierarchy_analysis, apply_name_fixes, build_hierarchy
from eia_map_product import build_map_product_df
from eia_map_remaining import add_mappings_and_corrections, extract_into_worksheets

if __name__ == '__main__':
    # get the original and proposed naive hierarchy for each symbol
    # the naive hierarchy is solely described by the manual mapping xls
    analysis_df = build_hierarchy_analysis()

    # some name fixes are applied to the new hierarchy
    final_df = apply_name_fixes(analysis_df)

    # save it because this relates source_key to hierarchy
    # and will be an input to the mapping exercise
    pathfile = os.path.join(path, file_for_mapping_preparation)
    final_df.to_pickle(pathfile)

    # drop dupes and save as csv for upload to mosaic
    build_hierarchy(final_df)

    # and now the product mapper
    build_map_product_df()

    # and finally, the remaining measures, units & locations
    # load the raw metadata (dont want the cleaned one)
    file = os.path.join(path, file_for_raw_metadata)
    metadata_df = pd.read_pickle(file)

    # one big metadata_df with extra columns for mappings and values being dictionaries that match the hierarchies
    df = add_mappings_and_corrections(metadata_df)

    # break out the mapping dictionaries into df columns and create dfs for upload
    xls_sheets = extract_into_worksheets(df)

    # save as xls
    pathfile = os.path.join(path, xlsx_for_map_remaining_result)
    with pd.ExcelWriter(pathfile) as writer:
        for sheet_name, df in xls_sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

