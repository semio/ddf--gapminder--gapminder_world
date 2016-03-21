# -*- coding: utf-8 -*-
"""update meta.json and en.json with latest data"""


import pandas as pd
import numpy as np
import re
import os
import json
from . ddf import to_concept_id


# update the translation file
en_source = '/Users/semio/src/work/Gapminder/vizabi/.data/translation/en.json'
concept_file = '../source/graph_settings - Indicators.csv'

concepts = pd.read_csv(concept_file)
k = concepts[concepts['ddf_id'] == u'———————————————————————'].index
concepts = concepts.drop(k)


with open(en_source) as f:
    enj = json.load(f)
    f.close()

concepts.columns = list(map(to_concept_id, concepts.columns))

trs = concepts[['ddf_id', 'ddf_name', 'ddf_unit', 'tooltip']].copy()
trs['key_1'] = 'indicator/'+trs['concept']
trs['key_2'] = 'unit/'+trs['concept']
trs['key_3'] = 'description/'+trs['concept']

trs1 = trs.drop('concept', axis=1).set_index('key_1')
trs2 = trs.drop('concept', axis=1).set_index('key_2')
trs3 = trs.drop('concept', axis=1).set_index('key_3')

enj.update(trs1['ddf_name'].dropna().to_dict())
enj.update(trs2['ddf_unit'].dropna().to_dict())
enj.update(trs3['tooltip'].dropna().to_dict())

with open(en_source, 'w') as f:
    json.dump(enj, f, indent=1, sort_keys=True)
    f.close()

# update the metadata
indb_file = '/Users/semio/src/work/Gapminder/vizabi/.data/waffles/metadata.json'
indb_0 = json.load(open(indb_file))

# 1. data domain and availability
dps = [x for x in os.listdir('../output/') if 'datapoints' in x]  # all datapoints files.
for i in dps:
    tc = re.match(r'ddf--datapoints--(.*)--by--geo--time.csv', i).groups()[0]

    df = pd.read_csv('../output/'+i, dtype={tc:float, 'time': int})
    dm = [float(df[tc].min()), float(df[tc].max())]
    av = [int(df['time'].min()), int(df['time'].max())]

    if tc in indb_0['indicatorsDB'].keys():
        indb_0['indicatorsDB'][tc].update({'domain':dm, 'availability': av})
    else:
        # print(k, ' is not in indicatorsDB, will insert to it')
        indb_0['indicatorsDB'][tc] = {'domain':dm, 'availability': av, 'use': 'indicator'}

# 2. source link
urls = concepts[['ddf_id', 'indicator_url']]
urls.columns = ['concept', 'sourcelink']

for k, v in urls.set_index('concept').to_dict('index').items():
    if k in indb_0['indicatorsDB'].keys():
        indb_0['indicatorsDB'][k].update(v)
    else:
        # print(k, ' is not in indicatorsDB, will insert to it')
        indb_0['indicatorsDB'][k] = v

# 3. indicator Tree
def get_menus(tree, upper=''):
    res = []
    if upper:
        lv1 = get_menus(tree)
        i = lv1.index(upper)
        if 'children' in tree['children'][i].keys():
            for t in tree['children'][i]['children']:
                res.append(t['id'])
    else:
        for t in tree['children']:
            res.append(t['id'])

    return res

# TODO: filter it first from indicatorDB
g = concepts.set_index('ddf_id').groupby(['menu_level1', 'menu_level_2']).groups
for k, v in g.items():

    l1, l2 = k

    if l1 is np.nan:
        for c in v:
            d = {'id': c}
            if d not in indb_0['indicatorsTree']['children']:
                indb_0['indicatorsTree']['children'].append(d)

    else:
        try:
            m1 = get_menus(indb_0['indicatorsTree'])
            i1 = m1.index(l1)
        except ValueError:
            indb_0['indicatorsTree']['children'].append({'id': l1, 'children': []})
            m1 = get_menus(indb_0['indicatorsTree'])
            i1 = m1.index(l1)

        if l2 is np.nan:
            for c in v:
                d = {'id': c}
                if d not in indb_0['indicatorsTree']['children'][i1]['children']:
                    indb_0['indicatorsTree']['children'][i1]['children'].append(d)

        else:
            try:
                m2 = get_menus(indb_0['indicatorsTree'], l1)
                i2 = m2.index(l2)
            except ValueError:
                if 'children' in indb_0['indicatorsTree']['children'][i1].keys():
                    indb_0['indicatorsTree']['children'][i1]['children'].append({'id': l2, 'children': []})
                else:
                    indb_0['indicatorsTree']['children'][i1]['children'] = [{'id': l2, 'children': []}]
                m2 = get_menus(indb_0['indicatorsTree'], l1)
                i2 = m2.index(l2)

            for c in v:
                d = {'id': c}
                if d not in indb_0['indicatorsTree']['children'][i1]['children'][i2]['children']:
                    indb_0['indicatorsTree']['children'][i1]['children'][i2]['children'].append(d)


# 4. en_old.json and metadata_old.json
trs = concepts[['ddf_id', 'ddf_name', 'ddf_unit', 'tooltip', 'old_ddf_id']].copy()
a = trs[['ddf_id', 'old_ddf_id']].ix[trs['old_ddf_id'].dropna().index].set_index('ddf_id')['old_ddf_id']

for k, v in a.iteritems():
    for i in ['indicator/', 'unit/', 'description/']:
        try:
            d = enj[i+k]
            del enj[i+k]
            enj[i+v] = d
        except KeyError:
            continue

with open('../output/en_old.json', 'w') as f:
    json.dump(enj, f, indent=1, sort_keys=True)
    f.close()

f_in = open('/Users/semio/src/work/Gapminder/vizabi/.data/waffles/metadata.json')
f_out = open('/Users/semio/src/work/Gapminder/vizabi/.data/waffles/metadata_old.json', 'w')

new = f_in.read()

for k, v in a.iteritems():
    if k in new:
        new = new.replace(k, v)
    else:
        print(k)

f_out.write(new)
f_out.close()

# TODO: what if I change one of the indicator names?
