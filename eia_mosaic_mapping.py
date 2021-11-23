'''
this script uses the raw metadata derived from mosaic tsdb
and generates mosaic mappings for upload
output is an xls for human consumption
'''

import os
import pandas as pd
from collections import namedtuple
from constants import TAB_DESCRIPTION, LOCATION, UNIT, DESCRIPTION, \
    path, file_for_raw_metadata, csv_for_hierarchy_result

REMAINING_DESCRIPTION = 'remaining description'
MAP_PRODUCT = 'map_product'
MAP_MEASURE = 'map_measure'
MAP_LOCATION = 'map_location'
MAP_SUBPRODUCT = 'map_sub_product'
MAP_UNIT = 'map_unit'

# hartree mapping keys
COUNTRY = 'country'
INTRA_COUNTRY_REGION = 'intra_country_region'
MEASURE = 'measure'
SUB_MEASURE = 'sub_measure'

# dictionary of mappings to list of string searches
# if a string search of the description field matches
# then the associated key is the mapping
product_mapping = {
    'crude oil': ['crude oil'],
    'motor gasoline blending components': [
        'conventional other gasoline blending components',
        'conventional cbob gasoline blending components',
        'conventional gtab gasoline blending components',
        'gasoline blending components'],
    'fuel ethanol': [
        'fuel ethanol'],
    'finished motor gasoline': [
        'reformulated motor gasoline',
        'reformulated rbob',
        'reformulated rbob with alcohol',
        'conventional motor gasoline',
        'finished motor gasoline',
        'finished conventional motor gasoline',
        'finished conventional motor gasoline with ethanol',
        'finished reformulated motor gasoline',
        'finished reformulated motor gasoline with ethanol',
        'motor gasoline',
    ],
    'kerosene type jet fuel': [
        'kerosene-type jet fuel'],
    'distillate fuel oil': [
        'distillate fuel oil',
    ],
    'residual fuel oil': ['residual fuel oil'],
    'propane/propylene': ['propane/propylene',
                          'propane and propylene']
}

# these are 1:1 string searches
sub_product_mapping = {'conventional': 'conventional',
                       'reformulated': 'reformulated',
                       'diesel': 'distillate fuel oil greater than 15 to 500 ppm sulfur',
                       'gasoil': 'distillate fuel oil 0 to 15 ppm sulfur',
                       'gtab': 'gtab',
                       'kerosene-type jet fuel': 'kerosene-type jet fuel'}

# this dictionary works in the opposite direction
# now if the key matches; the value is a dictionary that represents the mosaic mapping
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

# again if the key matches; the value is a dictionary that represents the mosaic mapping
measure_mapping = {
    'Imports': {MEASURE: 'imports', SUB_MEASURE: ''},  # this is new
    'Crude Oil Production': {MEASURE: 'imports', SUB_MEASURE: ''},
    'Days of Supply (Number of Days)': {MEASURE: 'supply', SUB_MEASURE: ''},
    'Ethanol Plant Production': {MEASURE: 'production', SUB_MEASURE: 'refinery output'},
    'Exports': {MEASURE: 'exports', SUB_MEASURE: ''},
    'Lower 48 Weekly Supply Estimates': {MEASURE: 'supply', SUB_MEASURE: ''},
    'Net Imports (Including SPR)': {MEASURE: 'imports', SUB_MEASURE: ''},
    'Product Supplied': {MEASURE: 'supply', SUB_MEASURE: ''},
    'Refiner and Blender Net Inputs': {MEASURE: 'input', SUB_MEASURE: 'refinery output'},
    'Refiner and Blender Net Production': {MEASURE: 'production', SUB_MEASURE: 'refinery output'},
    'Refiner Inputs and Utilization': {MEASURE: 'input', SUB_MEASURE: 'refinery output'},
    'Stocks': {MEASURE: 'stocks', SUB_MEASURE: 'closing'},
    'Ultra Low Sulfur Distillate': {MEASURE: 'supply', SUB_MEASURE: ''},
    'Weekly Preliminary Crude Imports by Top 10 Countries of Origin (ranking based on 2018 Petroleum Supply Monthly data)': {MEASURE: 'imports', SUB_MEASURE: ''},
}

# again if the key matches; the value is a dictionary that represents the mosaic mapping
unit_mapping = {
    'Thousand Barrels per Day': {'unit': 'kbd'},
    'Thousand Barrels': {'unit': 'kb'},
    'Thousand Barrels per Calendar Day': {'unit', 'kbd'},
    'Percent': {'unit': '%'},
    'Number of Days': {'unit': 'd'},  #  this one is new
}

# define some post-event corrections for any mapping
# the order of this list is important; corrections will be applied in sequence
SearchReplace = namedtuple('SearchReplace', ['search', 'replace'])
corrections_mapping = {
    MAP_MEASURE: [SearchReplace('capacity', 'Capacity'), SearchReplace('utilization', 'Utilization')],
}


if __name__ == '__main__':
    # load the metadata
    file = os.path.join(path, file_for_raw_metadata)
    metadata_df = pd.read_pickle(file)

    # standardise the label and the content
    metadata_df[DESCRIPTION] = metadata_df['Description'].str.lower()
    metadata_df.drop('Description', axis='columns')

    analysis_df = metadata_df.copy(deep=True)
    # append new columns
    analysis_df[REMAINING_DESCRIPTION] = ''
    analysis_df[MAP_PRODUCT] = ''
    analysis_df[MAP_MEASURE] = ''
    analysis_df[MAP_LOCATION] = ''
    analysis_df[REMAINING_DESCRIPTION] = analysis_df[DESCRIPTION].str.lower()

    # text search description to determine product
    for key, searches in product_mapping.items():
        for search in searches:
            # regex search returns series of boolean
            pattern = f'\\b({search})\\b'
            df = analysis_df[DESCRIPTION].str.contains(pattern)
            df.name = key + '|' + search
            # add a column with the boolean flag
            analysis_df = pd.concat([analysis_df, df], axis='columns')
            # remove the search text from the description so we can see whats left
            analysis_df[REMAINING_DESCRIPTION] = analysis_df[REMAINING_DESCRIPTION].str.replace(pattern, '', regex=True)
            # add the product mapping category
            analysis_df.loc[df.values, MAP_PRODUCT] = key
        # turn boolean into integer values so we can sum them
        analysis_df[[key + '|' + s for s in searches]] = analysis_df[[key + '|' + s for s in searches]].applymap(lambda x: int(x))

    # simple mapping
    analysis_df[MAP_MEASURE] = analysis_df[TAB_DESCRIPTION].map(measure_mapping)
    analysis_df[MAP_LOCATION] = analysis_df[LOCATION].map(location_mapping)
    analysis_df[MAP_UNIT] = analysis_df[UNIT].map(unit_mapping)

    # corrections to measures
    for correction in corrections_mapping[MAP_MEASURE]:
        df = analysis_df[DESCRIPTION].str.contains(correction.search)
        analysis_df.loc[df.values, MAP_MEASURE] = correction.replace

    # simple string search for sub-product mappings
    for key, search in sub_product_mapping.items():
        df = analysis_df[DESCRIPTION].str.contains(search)
        analysis_df.loc[df.values, MAP_SUBPRODUCT] = key

    columns = [TAB_DESCRIPTION, LOCATION, DESCRIPTION, REMAINING_DESCRIPTION, MAP_PRODUCT, MAP_SUBPRODUCT, MAP_MEASURE, MAP_LOCATION, MAP_UNIT]
    report_df = analysis_df[columns]

    # save as xls
    pathfile = os.path.join(path, csv_for_hierarchy_result)
    with pd.ExcelWriter(pathfile) as writer:
        report_df.to_excel(writer, sheet_name='mapping_result')
