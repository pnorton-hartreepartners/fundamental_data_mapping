import os
import pandas as pd
from oil_data_etl.common.model.Hierarchy import Hierarchy
from oil_data_etl.default_entities.geo import getDefaultHierarchy as geoHierarchy
from oil_data_etl.default_entities.products import getDefaultHierarchy as productsHierarchy
from oil_data_etl.default_entities.measures import getDefaultHierarchy as measuresHierarchy


if __name__ == '__main__':
    p = productsHierarchy()
    df = p.exportToDF(compact=True)

    keys = [c for c in df.columns if c[0] != '#']

    # explore
    for e in p.edges:
        if e['parent'] == 'ROOT:ROOT':
            print(e)

    # this one is interesting
    # {'parent': 'ROOT:ROOT', 'child': 'product_group:gasoline & naphtha'}

    aa = p.getAttributes(node='product_group:gasoline & naphtha')
    print(aa)
    # {'conversion': 8.45, 'source_unit': 'bbl', 'destination_unit': 'mt'}

    ee = p.getEntity(node='product_group:gasoline & naphtha')
    print(ee)
    # {'node': 'product_group:gasoline & naphtha', 'conversion': 8.45, 'source_unit': 'bbl', 'destination_unit': 'mt'}

    dd = p.getDescendants(node='product_group:gasoline & naphtha', strict=False)
    print(dd)
    # ['product_group:gasoline & naphtha', 'product:blending components', 'sub_product:blending components@reformulated', 'product_grade:blending components@reformulated@rbob', 'sub_product:blending components@conventional', 'product_grade:blending components@conventional@cbob', 'product_grade:gtab', 'product:gasoline', 'sub_product:gasoline@reformulated', 'product_grade:gasoline@reformulated@rbob', 'sub_product:gasoline@conventional', 'product_grade:gasoline@conventional@cbob', 'contract:rbob', 'contract_spec:nymex rbob', 'product_grade:a', 'product_grade:c', 'product:naphtha']

    ee = [(d.split(':')[0], d.split(':')[1]) for d in dd]
    descendents = [{e[0]:e[1]} for e in ee]

    print('raw ordering')
    for descendent in descendents:
        print(descendent.items())

    print('re-ordered list starts here')
    categories = ['product_group', 'product', 'sub_product', 'product_grade', 'contract', 'contract_spec']
    for category in categories:
        for descendent in descendents:
            if list(descendent.keys()) == [category]:
                print(descendent)


    print('hello world')
