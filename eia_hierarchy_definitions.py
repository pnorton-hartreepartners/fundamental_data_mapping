'''
edits to programmatically derived trees
'''

manually_remove_symbols = [
       # ===========================
       # US stocks

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

       # ===========================
       # US imports

       'WTTIMUS2',  # total
       'W_EPPK_IM0_NUS-Z00_MBBLD',  # kerosene
       'W_EPL0XP_IM0_NUS-Z00_MBBLD',  # ngpl/lrg
]

manually_remove_texts = ['Total Products']
