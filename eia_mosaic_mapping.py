import os
import pandas as pd
from constants import path, SOURCE_KEY, TAB_DESCRIPTION, LOCATION, \
    file_for_cleaned_metadata, file_for_mapping_preparation, DESCRIPTION, \
    numbers_as_words


def get_locations_mapper_df(df):
    df.reset_index(inplace=True)

    # the US locations
    mask = df[LOCATION] == 'U.S.'
    df = df[mask]

    # the other locations
    self_df = df.copy(deep=True)
    self_join_columns = [TAB_DESCRIPTION, DESCRIPTION]
    self_df = self_df[[LOCATION, SOURCE_KEY] + self_join_columns]

    df_result = pd.merge(df, self_df,
                   how='inner', left_on=self_join_columns, right_on=self_join_columns,
                   suffixes=['_US', None])
    df_result.set_index(SOURCE_KEY, drop=True, inplace=True)
    return df_result[[SOURCE_KEY + '_US', LOCATION]]


def get_hierarchy_for_all_locations_df(hierarchy_df, location_mapper_df):
    df = pd.merge(hierarchy_df['new_hierarchy'], location_mapper_df, left_index=True, right_on=SOURCE_KEY + '_US')
    return df['new_hierarchy'].to_frame()


def hierarchy_to_list(df):
    df['list'] = df['new_hierarchy'].str.split(pat='|').apply(lambda x: [e.strip() for e in x]).tolist()
    return df


def create_mosaic_mapping_df(df):
    for i, row in df.iterrows():
        # find the rightmost column index and value for leaf
        max_column = len(row['list']) - 1
        max_column_label = numbers_as_words[max_column]
        max_value = row['list'][max_column]
        df.at[i, 'leaf'] = f'{max_column_label}:{max_value}'
    return df


if __name__ == '__main__':

    # contains hierarchy for united states symbols only
    pathfile = os.path.join(path, file_for_mapping_preparation)
    hierarchy_df = pd.read_pickle(pathfile)

    # get locations from here
    pathfile = os.path.join(path, file_for_cleaned_metadata)
    metadata_df = pd.read_pickle(pathfile)

    # extend to all locations (from just united states)
    location_mapper_df = get_locations_mapper_df(df=metadata_df)
    all_locations_hierarchy_df = get_hierarchy_for_all_locations_df(hierarchy_df=hierarchy_df, location_mapper_df=location_mapper_df)

    # naive mapping using just the leaf notation (not the full branch)
    all_locations_hierarchy_df = hierarchy_to_list(df=all_locations_hierarchy_df)
    mapping_df = create_mosaic_mapping_df(df=all_locations_hierarchy_df)

