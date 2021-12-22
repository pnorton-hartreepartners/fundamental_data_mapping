import os
import pandas as pd
from constants import path, file_for_cleaned_metadata, xlsx_for_map_product_result, SOURCE_KEY, file_for_scrape_result, \
    xlsx_for_map_product_analysis, LOCATION, TAB_DESCRIPTION, DESCRIPTION


def build_mapping_healthcheck_df():
    # metadata
    pathfile = os.path.join(path, file_for_cleaned_metadata)
    metadata_df = pd.read_pickle(pathfile)

    # mosaic mapping
    pathfile = os.path.join(path, xlsx_for_map_product_result)
    map_product_df = pd.read_excel(pathfile)
    map_product_df.set_index(keys=SOURCE_KEY, drop=True, inplace=True)

    # original scrape
    pathfile = os.path.join(path, file_for_scrape_result)
    scrape_result_df = pd.read_pickle(pathfile)

    # join it all up
    both_df = pd.merge(metadata_df, map_product_df, how='outer', left_index=True, right_index=True)
    all_df = pd.merge(both_df, scrape_result_df, how='outer', left_index=True, right_index=True)

    # pivot table
    pivot_df = all_df.reset_index().pivot_table(values=SOURCE_KEY,
                                                index=[TAB_DESCRIPTION, DESCRIPTION, 'mosaic_upload', 'year_end'],
                                                columns=[LOCATION],
                                                aggfunc='count',
                                                fill_value=0)

    # check if we can swap source key to description
    groupby_columns = [LOCATION, 'raw_description']
    assert both_df.reset_index().groupby(groupby_columns)[SOURCE_KEY].transform('nunique').max() == 1

    # save file for visual check
    pathfile = os.path.join(path, xlsx_for_map_product_analysis)
    with pd.ExcelWriter(pathfile) as writer:
        all_df.to_excel(writer, sheet_name='mapping_analysis')
        pivot_df.to_excel(writer, sheet_name='mapping_pivot')
        map_product_df.to_excel(writer, sheet_name='mosaic_upload')


if __name__ == '__main__':
    build_mapping_healthcheck_df()
