import os
import pandas as pd
from constants import path, SOURCE_KEY, TAB_DESCRIPTION, LOCATION, \
    file_for_cleaned_metadata, file_for_mapping_preparation


def get_all_locations_df(hierarchy_df, metadata_df):
    metadata_df.reset_index(inplace=True)
    pivot_df = metadata_df.reset_index().pivot(index=[TAB_DESCRIPTION, 'Description'], columns=LOCATION, values=SOURCE_KEY)
    return pivot_df


def build_naive_leaf_mapping(hierarchy_df):
    return


if __name__ == '__main__':

    # contains hierarchy for united states symbols only
    pathfile = os.path.join(path, file_for_mapping_preparation)
    hierarchy_df = pd.read_pickle(pathfile)

    # get locations from here
    pathfile = os.path.join(path, file_for_cleaned_metadata)
    metadata_df = pd.read_pickle(pathfile)

    # extend to all locations (from just united states)
    hierarchy_df = get_all_locations_df(hierarchy_df, metadata_df)

    # naive mapping using just the leaf notation (not the full branch)
    mapping_df = build_naive_leaf_mapping(hierarchy_df)





