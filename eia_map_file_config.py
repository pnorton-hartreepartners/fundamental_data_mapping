'''
not implemented; intended to save manual xls hacking
'''

xls_mapper_headers = {'Info': {'STRATEGY': 'default',
                               'SOURCE': 'eia-weekly'
                               },
                      'Constants': {'NAME': ['VALUE', 'ENTITY PREFIX', 'NAME PREFIX'],
                                    'data_classification': ['fundamental', '', ''],
                                    'frequency': ['weekly', '', '']
                                    },
                      'Table_1': {'NAME': 'GEO',
                                  'ENTITY PREFIX': 'geo/',
                                  'NAME PREFIX': '',
                                  'Location': 'GEO'
                                  },
                      'Table_2': {'NAME': 'PRODUCT',
                                  'ENTITY PREFIX': 'productseia/',
                                  'NAME PREFIX': '',
                                  'Location': 'PRODUCT'
                                  },
                      'Table_3': {'NAME': 'MEASURE',
                                  'ENTITY PREFIX': 'measures/',
                                  'NAME PREFIX': '',
                                  'Location': 'MEASURE'
                                  },
                      'Table_4': {'NAME': 'unit',
                                  'ENTITY PREFIX': '',
                                  'NAME PREFIX': '',
                                  'Location': 'unit'
                                  },
}
