'''
functions for building and using metadata
'''

from constants import SOURCE_KEY


def get_all_metadata_for_symbol(metadata_df, source_key):
    return list(metadata_df.loc[metadata_df[SOURCE_KEY] == source_key].values[0])


def get_single_metadata_dict_for_all_symbols(metadata_df, label):
    return dict(zip(metadata_df.index, metadata_df[label].values))


def get_metadata_df(df, columns):
    metadata_df = df[columns].drop_duplicates()
    metadata_df.set_index(SOURCE_KEY, drop=True, inplace=True)
    return metadata_df

