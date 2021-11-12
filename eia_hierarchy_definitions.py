'''
generates a hierarchy as a dictionary
for consumption by timeseries analysis
'''
import pandas as pd
import os
import datetime as dt
from constants import path, file_for_scrape_result, TAB_DESCRIPTION, xlsx_for_cleaned_metadata, \
       file_for_cleaned_metadata, LOCATION

# ======================================
# import the scrape result
pathfile = os.path.join(path, file_for_scrape_result)
scrape_df = pd.read_pickle(pathfile)

# import the metadata
pathfile = os.path.join(path, file_for_cleaned_metadata)
metadata_df = pd.read_pickle(pathfile)

# join it up
scrape_df = scrape_df.join(metadata_df)

# ======================================
# filter for headline stocks data ie level zero

# we dont want expired symbols
current_year = dt.date.today().year

# weird pandas feature == operator doesnt work here
mask = scrape_df['level'].eq(0) \
       & scrape_df[TAB_DESCRIPTION].eq('Stocks') \
       & scrape_df['year_end'].eq(current_year) \
       & scrape_df[LOCATION].eq('U.S.')

# the first symbol in the list is the total of everything; so make it the key
list_of_symbols = list(scrape_df[mask].index)
key = list_of_symbols.pop(0)

# remove this as its not part of the hierarchy
# Total Crude Oil and Petroleum Products (Excl. SPR)
list_of_symbols.remove('WTESTUS1')

hierarchy_dict_us_stocks = {key: list_of_symbols}

us_stocks_remove_symbols = [
       # Total Crude Oil and Petroleum Products
       'WTTSTUS1',
       'WTESTUS1',
       # Alaska In-Transit
       'W_EPC0_SKA_NUS_MBBL',
       # Propylene (Nonfuel Use)
       'WPLSTUS1',
       # children of 'Other Oils (Excluding Ethanol)'
       'WUOSTUS1',
       'W_EPPK_SAE_NUS_MBBL',
       'W_EPPA_SAE_NUS_MBBL',
       'W_EPL0XP_SAE_NUS_MBBL',
]



'''
this is old manual attempt; can probably throw away
hierarchy_dict_us_stocks = {
    # ======================================
    'WTTSTUS1':
        ['WCRSTUS1',  # Crude Oil (Including SPR)
         'WGTSTUS1',  # Total Motor Gasoline
         'W_EPOOXE_SAE_NUS_MBBL',  # Fuel Ethanol
         'WKJSTUS1',  # Kerosene-Type Jet Fuel
         'WDISTUS1',  # Distillate Fuel Oil
         'WRESTUS1',  # Residual Fuel Oil
         'WPRSTUS1',  # Propane/Propylene (Excl. Propylene at Terminal)
         'W_EPPO6_SAE_NUS_MBBL'  # Other Oils (Excluding Ethanol)
         ],
    # ======================================
    # Crude Oil (Including SPR)
    'WCRSTUS1':
        ['WCESTUS1',  # Commercial (Excl. Lease Stock )
        'WCSSTUS1',   # SPR
         ],
    # ======================================
    # Total Motor Gasoline
    'WGTSTUS1':
        ['WGFSTUS1',
         'WBCSTUS1'],
    # ======================================
    # Finished Motor Gasoline
    'WGFSTUS1':  # level 1
        ['WGRSTUS1',
         'WG4ST_NUS_1'],
    'WGRSTUS1':  # level 2
        ['WG1ST_NUS_1',
         'WG3ST_NUS_1'],
    'WG4ST_NUS_1':  # level 2
        ['WG5ST_NUS_1',
         'WG6ST_NUS_1'],
    'WG5ST_NUS_1':  # level 3
        ['W_EPM0CAL55_SAE_NUS_MBBL',
         'W_EPM0CAG55_SAE_NUS_MBBL'],
    # ======================================
    # Motor Gasoline Blending Components
    'WBCSTUS1':  # level 1
        ['W_EPOBGRR_SAE_NUS_MBBL',
         'WO6ST_NUS_1',
         'WO7ST_NUS_1',
         'WO9ST_NUS_1'],
    'W_EPOBGRR_SAE_NUS_MBBL':  # level 2
        ['WO4ST_NUS_1',
         'WO3ST_NUS_1'],

    # ======================================
    # Distillate Fuel Oil
    'WDISTUS1':
        ['WD0ST_NUS_1',
         'WD1ST_NUS_1',
         'WDGSTUS1']

}

other_dict = {
    'WBCRI_NUS_2':  # 'Refiner and Blender Net Input of Gasoline Blending Components'
        ['W_EPOBGRR_YIR_NUS_MBBLD',
         'WO6RI_NUS_2',
         'WO7RI_NUS_2',
         'WO9RI_NUS_2'],
    'W_EPPO6_SAE_NUS_MBBL':  # Ending Stocks of Other Oils; this one doesn't add up
        ['W_EPPA_SAE_NUS_MBBL',
         'W_EPPK_SAE_NUS_MBBL',
         'W_EPL0XP_SAE_NUS_MBBL',
         'WUOSTUS1'],
    'WTTSTUS1':  # Ending Stocks of Crude Oil and Petroleum Products; at the most granular level
        ['WCESTUS1',
         'WCSSTUS1',
         'WG1ST_NUS_1',
         'WG3ST_NUS_1',
         'W_EPM0CAL55_SAE_NUS_MBBL',
         'W_EPM0CAG55_SAE_NUS_MBBL',
         'WG6ST_NUS_1',
         'W_EPOBGRR_SAE_NUS_MBBL',
         'WO4ST_NUS_1',
         'WO3ST_NUS_1',
         'WO6ST_NUS_1',
         'WO7ST_NUS_1',
         'WO9ST_NUS_1',
         'W_EPOOXE_SAE_NUS_MBBL',
         'WKJSTUS1',
         'WD0ST_NUS_1',
         'WD1ST_NUS_1',
         'WDGSTUS1',
         'WRESTUS1',
         'WPRSTUS1',
         'W_EPPO6_SAE_NUS_MBBL']}
'''
