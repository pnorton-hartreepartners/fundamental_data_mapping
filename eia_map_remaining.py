import os
import pandas as pd
from collections import namedtuple, OrderedDict
from constants import TAB_DESCRIPTION, LOCATION, UNIT, DESCRIPTION, \
    path, xlsx_for_map_remaining_result, file_for_raw_metadata

MAP_MEASURE = 'map_measure'
MAP_LOCATION = 'map_location'
MAP_UNIT = 'map_unit'

# hartree mapping keys
COUNTRY = 'country'
INTRA_COUNTRY_REGION = 'intra_country_region'
MEASURE = 'measure'
SUB_MEASURE = 'sub_measure'

# use the field in the metadata to determine the mapping measure for upload to mosaic
column_mapping = [
    # eia table column to mapping data... and the mosaic table to which we'll add it
    (TAB_DESCRIPTION, MAP_MEASURE, 'measure_upload'),
    (UNIT, MAP_UNIT, 'unit_upload'),
    (LOCATION, MAP_LOCATION, 'location_upload'),
]

location_mapping = {
    'East Coast (PADD 1)': {INTRA_COUNTRY_REGION: 'PADD I'},
    'Gulf Coast (PADD 3)': {INTRA_COUNTRY_REGION: 'PADD III'},
    'Midwest (PADD 2)': {INTRA_COUNTRY_REGION: 'PADD II'},
    'Midwest (PADD2)': {INTRA_COUNTRY_REGION: 'PADD II'},
    'West Coast (PADD 5)': {INTRA_COUNTRY_REGION: 'PADD IV & V'},
    'Rocky Mountain (PADD 4)': {INTRA_COUNTRY_REGION: 'PADD IV & V'},
    'Rocky Mountains (PADD 4)': {INTRA_COUNTRY_REGION: 'PADD IV & V'},
    'Lower 48 States': {COUNTRY: 'United States', INTRA_COUNTRY_REGION: ''},
    'U.S.': {COUNTRY: 'United States'},
}
location_mapping = {k: OrderedDict(v) for k, v in location_mapping.items()}

# again if the key matches; the value is a dictionary that represents the mosaic mapping
measure_mapping = {
    'Imports': {MEASURE: 'imports'},
    'Crude Oil Production': {MEASURE: 'production'},
    'Days of Supply (Number of Days)': {MEASURE: 'supply'},
    'Ethanol Plant Production': {MEASURE: 'production', SUB_MEASURE: 'refinery output'},
    'Exports': {MEASURE: 'exports'},
    'Lower 48 Weekly Supply Estimates': {MEASURE: 'supply'},
    'Net Imports (Including SPR)': {MEASURE: 'imports'},
    'Product Supplied': {MEASURE: 'supply'},
    'Refiner and Blender Net Inputs': {MEASURE: 'input', SUB_MEASURE: 'refinery output'},
    'Refiner and Blender Net Production': {MEASURE: 'production', SUB_MEASURE: 'refinery output'},
    'Refiner Inputs and Utilization': {MEASURE: 'input', SUB_MEASURE: 'refinery output'},
    'Stocks': {MEASURE: 'stocks', SUB_MEASURE: 'closing'},
    'Ultra Low Sulfur Distillate': {MEASURE: 'supply'},
    'Weekly Preliminary Crude Imports by Top 10 Countries of Origin (ranking based on 2020 Petroleum Supply Monthly data)': {MEASURE: 'imports'},
}
measure_mapping = {k: OrderedDict(v) for k, v in measure_mapping.items()}

# again if the key matches; the value is a dictionary that represents the mosaic mapping
unit_mapping = {
    'Thousand Barrels per Day': {'unit': 'kbd'},
    'Thousand Barrels': {'unit': 'kb'},
    'Thousand Barrels per Calendar Day': {'unit': 'kbd'},
    'Percent': {'unit': '%'},
    'Number of Days': {'unit': 'd'},  #  this one is new
}
unit_mapping = {k: OrderedDict(v) for k, v in unit_mapping.items()}


# define some post-event corrections for any mapping
# the order of this list is important; corrections will be applied in sequence
SearchReplace = namedtuple('SearchReplace', ['search', 'replace'])
corrections_mapping = {
    MAP_MEASURE: [SearchReplace('capacity', 'Capacity'), SearchReplace('utilization', 'Utilization')],
}


def add_mappings_and_corrections(df):
    # simple mapping
    df[MAP_MEASURE] = df[TAB_DESCRIPTION].map(measure_mapping)
    df[MAP_LOCATION] = df[LOCATION].map(location_mapping)
    df[MAP_UNIT] = df[UNIT].map(unit_mapping)

    # corrections to measures
    for correction in corrections_mapping[MAP_MEASURE]:
        mask = df[DESCRIPTION].str.contains(correction.search)
        df.loc[mask, MAP_MEASURE] = correction.replace
    return df


def extract_into_worksheets(df):
    def myfunc(x):
        try:
            # get the last item in the ordered dict
            k, v = next(reversed(x.items()))
            return f'{k}:{v}'
        except KeyError:
            return ''

    worksheets = {}
    for map_from_column, map_to_column, table in column_mapping:
        df[table] = df[map_to_column].apply(myfunc)
        worksheets[map_to_column] = df[[map_from_column, table]].drop_duplicates(ignore_index=True)
    return worksheets


if __name__ == '__main__':
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

