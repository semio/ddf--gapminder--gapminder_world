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
    real = idt[idt['-t-ind'] == s]['-t-name'].iloc[0]
    cid = concepts[concepts['name'] == real]['concept_id'].iloc[0]
    return cid


def rename_geo(s):
    return geo_.get_value(s, 'ISO3dig_ext')


# begin

# reading files
concepts = pd.read_csv('../source/graph_settings - Indicators.csv')
gps = pd.read_json('../../waffle-server-importers-exporters-world-legacy-with-data/data/out/gw/meta/area_categorizarion.json')
geo = pd.read_excel('../../waffle-server-importers-exporters-world-legacy-with-data/data/synonym/country_synonyms.xlsx')
idt = pd.read_json('../../waffle-server-importers-exporters-world-legacy-with-data/data/out/gw/meta/quantities.json')
reg_f = open('../../waffle-server-importers-exporters-world-legacy-with-data/data/synonym/regions.json')
regs = json.load(reg_f)

# continuous concepts
dsc = concepts.columns
concepts.columns = list(map(to_concept_id, concepts.columns))
concepts['concept_type'] = 'measure'
concepts = concepts.drop('download', axis=1)
concepts = concepts.iloc[:, [5, 0, 1,2,3,4,6,7]]
concepts = concepts.rename(columns={'ddf_id':'concept'})
dsc.drop('ddf_id')
cc = concepts[['concept', 'name', 'concept_type', 'tooltip', 'indicator_url']].copy()
k = concepts[concepts.concept == u'———————————————————————'].index
cc = cc.drop(k)
cc['drill_up'] = np.nan
cc['domain'] = np.nan

# country entities
geo_ = geo[['ISO3dig_ext', 'Gwid']]
geo_ = geo_.set_index('Gwid')
geo_2 = geo.set_index('Gwid').drop('ISO3dig_ext', axis=1)

path = '../../waffle-server-importers-exporters-world-legacy-with-data/data/out/gw/indicators/'
fs = os.listdir(path)

for f in fs[1:]:
    p = os.path.join(path, f)

    col = f[:-5]
    col_r = rename_col(col)

    d = pd.read_json(p)

    if 'geo' in d.columns:
        d['geo'] = d['geo'].apply(rename_geo)
        d = d.rename(columns={col:col_r})  #ok?
        d.to_csv('../output/ddf--datapoints--'+col_r+'--by--geo--time.csv', index=False)
    else:
        print(col, ':', col_r)

regd = {}

for i in regs:
    regd[i.get(list(i.keys())[0])] = list(i.keys())[0]

country = geo_.copy()
# country['geographic_regions'] = np.nan
# country['income_groups'] = np.nan
# country['landlocked'] = np.nan
# country['g77_and_oecd_countries'] = np.nan
# country['geographic_regions_in_4_colors'] = np.nan
# country['main_religion_2008'] = np.nan

for i, n in gps.n.apply(to_concept_id).iteritems():

    res = {}

    for k, v in gps.iloc[i]['groupings'].items():
    #     print(regd[k])
        for c in v:
            if c:
                if i == 4:
                    res[c] = to_concept_id(regd[k], sub='[/ -\.\*";\[\]]+', sep='')
                else:
                    res[c] = to_concept_id(regd[k], sub='[/ -\.\*";\[\]]+')

    ser = pd.Series(res)
    country[n] = ser

country2 = pd.concat([country, geo_2], axis=1)
country2 = country2.rename(columns={'NAME': 'Upper Case Name', 'Use Name': 'Name'})
ccs = country2.columns[8:]
country2 = country2.reset_index()
country2 = country2.rename(columns={'ISO3dig_ext':'country', 'Gwid':'gwid'})
country2.columns = list(map(to_concept_id, country2.columns))
country2['is--country'] = 'TRUE'
country2.to_csv('../output/ddf--entities--geo--country.csv', index=False)

# discrete concepts
dc = pd.DataFrame([], columns=cc.columns)
dcl = list(map(to_concept_id, gps.n.values))
ccs_id = list(map(to_concept_id, ccs))  # not defined
dcl_ = np.r_[dcl, ['geo', 'country','time', 'name', 'gwid'], [x+'_id' for x in dcl], ccs_id, ['tooltip', 'indicator_url']]
dcl_2 = np.r_[gps.n.values, ['Geo', 'Country','Time', 'Name', 'Gwid'], [x+' id' for x in gps.n.values], ccs, ['tooltip', 'indicator_url']]
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
c_all.to_csv('./output/ddf--concepts.csv', index=False)

# entities sets
for i, n in gps.n.apply(to_concept_id).iteritems():
    df = pd.DataFrame([], columns=[n, 'name', n+'_id', 'is--'+n])
    df[n+'_id'] = gps.iloc[i]['groupings'].keys()
    if i == 4:
        df[n] = df[n+'_id'].apply(lambda x: to_concept_id(regd[x], sub='[/ -\.\*";\[\]]+', sep=''))
    else:
        df[n] = df[n+'_id'].apply(lambda x: to_concept_id(regd[x], sub='[/ -\.\*";\[\]]+'))

    df['name'] = df[n+'_id'].apply(lambda x: regd[x])
    df['is--'+n] = 'TRUE'

    df.to_csv('../output/ddf--entities--geo--'+n+'.csv', index=False)


# other things including meta.json and translation json file.
# commoned out because the en.json and meta.json is updated so script below are
# not working now.

# en = open('/Users/semio/src/work/Gapminder/vizabi/.data/translation/en.json')
# enj = json.load(en)

# idts = cc[['concept', 'name']].set_index('concept').to_dict()['name']
# idts_new = {}

# for k, v in idts.items():
#     idts_new['indicator/'+k] = v

# enj.update(idts_new)

# en = open('/Users/semio/src/work/Gapminder/vizabi/.data/translation/en.json', 'w')
# json.dump(enj, en, indent=1, sort_keys=True)


# indb = concepts[['concept', 'scale', 'indicator_url']].drop(k)
# indb.columns = ['concept', 'scale', 'sourcelink']
# indb = indb.set_index('concept')
# indb['use'] = 'indicator'

# def scal(s):
#     if s is not np.nan:
#         if s == 'lin':
#             return ['linear']
#         else:
#             return [s]
#     else:
#         return s

# indb['scales'] = indb['scale'].apply(scal)
# indb.drop('scale', axis=1).to_dict('index')
