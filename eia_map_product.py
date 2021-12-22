'''
the initial hierarchy and mapping data was only for the US location
now expand for all locations
then create the required syntax required by the mosaic mapper
'''

import os
import pandas as pd
from constants import path, SOURCE_KEY, TAB_DESCRIPTION, LOCATION, \
    file_for_cleaned_metadata, file_for_mapping_preparation, DESCRIPTION, \
    numbers_as_words, xlsx_for_map_product_result


def get_locations_mapper_df(metadata_df):
    # we need to map U.S. to the other locations
    # so here we self-join on the columns 'TabDescription' and 'Description' of our metadata
    # and return a df with the location mapping only
    metadata_df.reset_index(inplace=True)
    self_df = metadata_df.copy(deep=True)

    # the US locations
    mask = metadata_df[LOCATION] == 'U.S.'
    metadata_df = metadata_df[mask]

    # the other locations
    # PRODUCT_KEY is inconsistent so we correct descriptions when we build metadata
    # and use the clean result here
    self_join_columns = [TAB_DESCRIPTION, DESCRIPTION]
    self_df = self_df[[LOCATION, SOURCE_KEY] + self_join_columns]

    mapper_df = pd.merge(metadata_df, self_df,
                         how='inner', left_on=self_join_columns, right_on=self_join_columns,
                         suffixes=['_US', None])
    return mapper_df.set_index(SOURCE_KEY, drop=True)


def get_hierarchy_for_all_locations_df(hierarchy_df, location_mapper_df):
    # using our mapper df we can copy the hierarchy definition for the US, to all locations
    return pd.merge(hierarchy_df['new_hierarchy'], location_mapper_df, left_index=True, right_on=SOURCE_KEY + '_US')


def hierarchy_to_list(df):
    df['list'] = df['new_hierarchy'].str.split(pat='|').apply(lambda x: [e.strip() for e in x]).tolist()
    return df


def create_leaf_notation_df(df):
    # function returns the simple leaf notation
    for i, row in df.iterrows():
        max_column = len(row['list']) - 1
        max_column_label = numbers_as_words[max_column]
        max_value = row['list'][max_column]
        df.at[i, 'leaf'] = f'{max_column_label}:{max_value}'
    return df


def apply_path_corrections(df, corrections):
    # mosaic adds this nomenclature to differentiate using some portion of the path to the leaf
    for pattern, backward in corrections:
        mask = df['mosaic_upload'] == pattern
        for i, row in df[mask].iterrows():
            l = len(row['list'])
            max_column = numbers_as_words[l - backward + 1]
            branch_str = '@'.join(row['list'][-backward:])
            df.loc[mask, 'mosaic_upload'] = f'{max_column}:{branch_str}'
    return df


def build_map_product_df():
    # contains hierarchy for united states symbols only
    pathfile = os.path.join(path, file_for_mapping_preparation)
    hierarchy_df = pd.read_pickle(pathfile)

    # get locations from here
    pathfile = os.path.join(path, file_for_cleaned_metadata)
    metadata_df = pd.read_pickle(pathfile)

    # extend to all locations (from just united states)
    locations_mapper_df = get_locations_mapper_df(metadata_df=metadata_df)
    df = locations_mapper_df[[SOURCE_KEY + '_US', LOCATION]]
    all_locations_hierarchy_df = get_hierarchy_for_all_locations_df(hierarchy_df=hierarchy_df,
                                                                    location_mapper_df=df)

    # naive mapping using just the leaf notation (not the full branch)
    df = hierarchy_to_list(df=all_locations_hierarchy_df)
    mapping_df = create_leaf_notation_df(df=df['list'].to_frame())

    # start creating the upload
    mapping_df['mosaic_upload'] = mapping_df['leaf']

    # apply corrections due to mosaic hierarchy logic
    corrections = [('five:Blended with Fuel Ethanol', 2)]
    sourcekey_mapping_df = apply_path_corrections(mapping_df, corrections)

    # more contortions; can we map to description instead?
    metadata_df.set_index(SOURCE_KEY, drop=True, inplace=True)
    both_df = pd.merge(metadata_df, sourcekey_mapping_df, how='outer', left_index=True, right_index=True)
    description_mapping_df = both_df[['raw_description', 'mosaic_upload']]
    description_mapping_df = description_mapping_df.drop_duplicates(ignore_index=True)
    description_mapping_df.set_index('raw_description', drop=True, inplace=True)

    # save file for upload
    pathfile = os.path.join(path, xlsx_for_map_product_result)
    with pd.ExcelWriter(pathfile) as writer:
        sourcekey_mapping_df['mosaic_upload'].to_excel(writer, sheet_name='sourcekey_mapping')
        description_mapping_df['mosaic_upload'].to_excel(writer, sheet_name='description_mapping')


if __name__ == '__main__':
    build_map_product_df()
