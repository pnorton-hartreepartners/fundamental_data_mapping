import os
import pandas as pd
from constants import path, file_for_cleaned_metadata, xlsx_for_map_product_result, SOURCE_KEY, file_for_scrape_result

if __name__ == '__main__':
    pathfile = os.path.join(path, file_for_cleaned_metadata)
    metadata_df = pd.read_pickle(pathfile)

    pathfile = os.path.join(path, xlsx_for_map_product_result)
    map_product_df = pd.read_excel(pathfile)
    map_product_df.set_index(keys=SOURCE_KEY, drop=True, inplace=True)

    pathfile = os.path.join(path, file_for_scrape_result)
    scrape_result_df = pd.read_pickle(pathfile)

    both_df = pd.merge(metadata_df, map_product_df, how='outer', left_index=True, right_index=True)
    mask = both_df['mosaic_upload'].isna()
    unmapped_df = both_df[mask]
    unmapped_df.to_clipboard()

    all_df = pd.merge(both_df, scrape_result_df, how='outer', left_index=True, right_index=True)
    all_df.to_clipboard()




    print()






