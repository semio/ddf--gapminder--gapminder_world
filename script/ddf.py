# -*- coding: utf-8 -*-
"""create ddf files for gapminder world data, from the origin
json version files.
"""

import pandas as pd
import numpy as np
import json
import re
import os

# configuration
# indicator name and their hash names. The hashs are used as file name in
# indicator json folder waffle-server-importers-exporters-world-legacy-with-data/data/out/gw/indicators/
idt_json = '../../waffle-server-importers-exporters-world-legacy-with-data/data/out/gw/meta/quantities.json'

# groupings, there are 5 ways of grouping of countries in the data set.
gps_json = '../../../ddf--gapminder_world/waffle-server-importers-exporters-world-legacy-with-data/data/out/gw/meta/area_categorizarion.json'

# Country properities
geo_file = '../../waffle-server-importers-exporters-world-legacy-with-data/data/synonym/country_synonyms.xlsx'

# Region properities
reg_file = '../../waffle-server-importers-exporters-world-legacy-with-data/data/synonym/regions.json'

# indicator properities
concept_file = '../source/graph_settings - Indicators.csv'

# data point source path
data_source = '../../waffle-server-importers-exporters-world-legacy-with-data/data/out/gw/indicators/'

# output dir
out_dir = '../output'


# reading the files above...
idt = pd.read_json(idt_json)
gps = pd.read_json(gps_json)
geo = pd.read_excel(geo_file)
regs = json.load(open(reg_file))
concepts = pd.read_csv(concept_file, encoding='utf8')


# helper functions
def to_concept_id(s, sub='[/ -\.\*";]+', sep='_'):
    '''convert a string to lowercase alphanumeric + underscore id for concepts'''

    if s is np.nan:
        return s

    s1 = re.sub(sub, sep, s.strip())
#     s1 = re.sub(r'\[.*\]', '', s1)
    s1 = s1.replace('\n', '')

    if s1[-1] == sep:  # remove the last underscore
        s1 = s1[:-1]

    return s1.lower()


def rename_col(s):
    '''map concept name to concept id'''
    real = idt[idt['-t-ind'] == s]['-t-name'].iloc[0]
    cid = concepts[concepts['name'] == real]['concept'].iloc[0]
#     print(real, ': ', cid)

    return cid


def rename_geo(s):
    """map gwid to iso code"""
    return geo_.get_value(s, 'ISO3dig_ext')

# Entities of country gourps
regd = {}  # a dictionary, which keys are region id and values are region names

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

    df.to_csv(os.path.join(out_dir, 'ddf--entities--geo--'+n+'.csv'), index=False, encoding='utf8')


# Entities of Country
# create groupings properities
geo_ = geo[['ISO3dig_ext', 'Gwid']]
geo_ = geo_.set_index('Gwid')
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

# now add other properities
geo_2 = geo.set_index('Gwid').drop('ISO3dig_ext', axis=1)
country2 = pd.concat([country, geo_2], axis=1)
country2 = country2.reset_index()
country2 = country2.rename(columns={'NAME': 'Upper Case Name', 'Use Name': 'Name', 'ISO3dig_ext': 'country'})
cnc = country2.columns[8:]  # save the column names before transformation. will be used below.
country2.columns = list(map(to_concept_id, country2.columns))
country2['is--country'] = 'TRUE'

c2c = ['country', 'gwid', 'name', 'geographic_regions', 'income_groups', 'landlocked',
       'g77_and_oecd_countries', 'geographic_regions_in_4_colors',
       'main_religion_2008', 'gapminder_list', 'alternative_1',
       'alternative_2', 'alternative_3', 'alternative_4_cdiac', 'pandg',
       'god_id', 'alt_5', 'upper_case_name', 'code', 'number', 'arb1', 'arb2',
       'arb3', 'arb4', 'arb5', 'arb6', 'is--country']

country2.loc[:, c2c].to_csv(os.path.join(out_dir, 'ddf--entities--geo--country.csv'), encoding='utf8', index=False)


# concepts
dsc = concepts.columns

concepts.columns = list(map(to_concept_id, concepts.columns))
concepts['concept_type'] = 'measure'
concepts = concepts.drop('download', axis=1)
concepts = concepts.iloc[:, [5, 0, 1,2,3,4,6,7]]
concepts = concepts.rename(columns={'ddf_id':'concept'})
concepts['concept_new'] = concepts['concept'].apply(to_concept_id)

cc = concepts[['concept', 'name', 'concept_type', 'tooltip', 'indicator_url']].copy()
k = concepts[concepts.concept == u'———————————————————————'].index
cc = cc.drop(k)
cc['drill_up'] = np.nan
cc['domain'] = np.nan

dc = pd.DataFrame([], columns=cc.columns)
dcl = list(map(to_concept_id, gps.n.values))
cnc_id = list(map(to_concept_id, cnc))

dcl_ = np.r_[dcl, ['geo', 'country','time', 'gwid'], [x+'_id' for x in dcl], cnc_id, ['tooltip', 'indicator_url']]
dcl_2 = np.r_[gps.n.values, ['Geo', 'Country','Time', 'Gwid'], [x+' id' for x in gps.n.values], cnc, ['tooltip', 'indicator_url']]

dc['concept'] = dcl_
dc['name'] = dcl_2

dc['concept_type'] = 'string'
dc.loc[:5, 'concept_type'] = 'entity_set'
dc.loc[6, 'concept_type'] = 'entity_domain'
dc.loc[7, 'concept_type'] = 'entity_set'
dc.loc[8, 'concept_type'] = 'time'

# dc['concept_type'] = 'string'
dc.loc[:5, 'domain'] = 'geo'

dc.loc[7, 'drill_up'] = dcl
dc.loc[7, 'domain'] = 'geo'

c_all = pd.concat([dc, cc])
c_all.to_csv(os.path.join(out_dir, 'ddf--concepts.csv'), index=False, encoding='utf8')


# Datapoints
# fs = os.listdir(data_source)
# for f in fs[1:]:
#     p = os.path.join(data_source, f)

#     col = f[:-5]
#     col_r = rename_col(col)

#     d = pd.read_json(p)

#     if 'geo' in d.columns:
#         d['geo'] = d['geo'].apply(rename_geo)
#         d = d.rename(columns={col: col_r})
#         d.to_csv(os.path.join(out_dir, 'ddf--datapoints--'+col_r+'--by--geo--time.csv'), index=False)
#     else:
#         continue
