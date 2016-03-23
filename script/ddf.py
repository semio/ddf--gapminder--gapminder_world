# -*- coding: utf-8 -*-
"""create ddf files for gapminder world data, from the origin
json version files.
"""

import pandas as pd
import numpy as np
import json
import re
import os
from common import to_concept_id


# helper functions

def rename_col(s, idt, concepts):
    '''map concept name to concept id'''
    real = idt[idt['-t-ind'] == s]['-t-name'].iloc[0]
    cid = concepts[concepts['name'] == real]['concept'].iloc[0]
    return cid


def rename_geo(s, geo_):
    """map gwid to iso code"""
    return geo_.get_value(s, 'ISO3dig_ext')  # geo_ will be defined below.

# Entities of country gourps

def extract_entities_groups(regs, gps):
    regd = {}  # a dictionary, which keys are region id and values are region names
    res = {}

    for i in regs:
        regd[i.get(list(i.keys())[0])] = list(i.keys())[0]

    for i, n in gps.n.apply(to_concept_id).iteritems():
        df = pd.DataFrame([], columns=[n, 'name', n+'_id', 'is--'+n])
        df[n+'_id'] = gps.iloc[i]['groupings'].keys()
        if i == 4:
            df[n] = df[n+'_id'].apply(lambda x: to_concept_id(regd[x], sub='[/ -\.\*";\[\]]+', sep=''))
        else:
            df[n] = df[n+'_id'].apply(lambda x: to_concept_id(regd[x], sub='[/ -\.\*";\[\]]+'))

        df['name'] = df[n+'_id'].apply(lambda x: regd[x])
        df['is--'+n] = 'TRUE'
        res[n] = df

        # df.to_csv(os.path.join(out_dir, 'ddf--entities--geo--'+n+'.csv'), index=False, encoding='utf8')
    return res

# Entities of Country
def extract_entities_country(regs, geo, gps, geo_sg):
    regd = {}
    for i in regs:
        regd[i.get(list(i.keys())[0])] = list(i.keys())[0]

    geo_ = geo[['ISO3dig_ext', 'Gwid']]
    geo_ = geo_.set_index('Gwid')
    geo_2 = geo.set_index('Gwid').drop('ISO3dig_ext', axis=1)

    country = geo_.copy()

    for i, n in gps.n.apply(to_concept_id).iteritems():

        res = {}

        for k, v in gps.iloc[i]['groupings'].items():
            for c in v:
                if c:
                    if i == 4:
                        res[c] = to_concept_id(regd[k], sub='[/ -\.\*";\[\]]+', sep='')
                    else:
                        res[c] = to_concept_id(regd[k], sub='[/ -\.\*";\[\]]+')

        ser = pd.Series(res)

        country[n] = ser

    country2 = pd.concat([country, geo_2], axis=1)
    country2 = country2.reset_index()
    country2 = country2.rename(columns={'NAME': 'Upper Case Name', 'Use Name': 'Name', 'ISO3dig_ext': 'country_2'})
    country2.columns = list(map(to_concept_id, country2.columns))
    country2['is--country'] = 'TRUE'

    country3 = geo_sg[['geo', 'world_4region', 'latitude', 'longitude', 'name']]
    country3 = country3.rename(columns={'geo': 'country'}).set_index('name')

    country4 = pd.concat([country2.set_index('name'), country3], axis=1)
    country4 = country4.reset_index()
    country4 = country4.rename(columns={'index': 'name'})
    country4 = country4.drop('country_2', axis=1)

    return country4

# TODO: below functions not completed yet.
# concepts
def extract_concepts():
    pass


# Datapoints
def extract_datapoints(data_source):
    fs = os.listdir(data_source)
    for f in fs[1:]:
        p = os.path.join(data_source, f)

        col = f[:-5]  # get indicator name
        col_r = rename_col(col)  # get indicator's id

        d = pd.read_json(p)

        if 'geo' in d.columns:
            d['geo'] = d['geo'].apply(rename_geo)
            d = d.rename(columns={col: col_r})
            d.to_csv(os.path.join(out_dir, 'ddf--datapoints--'+col_r+'--by--geo--time.csv'), index=False, encoding='utf8')
        else:
            print('passed empty data file for ', col_r)
