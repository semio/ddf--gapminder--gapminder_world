# -*- coding: utf-8 -*-
"""update meta.json and en.json with latest data"""


import pandas as pd
import numpy as np
import os
from common import to_concept_id, to_dict_dropna
from collections import OrderedDict


def update_enjson(enj, c_all, concepts):
    """update the existing en.json with new concepts.

    enj: source en.json
    c_all: ddf--concepts of this repo.
    concepts: graph settings file to get the menu levels indicator.
    """
    trs = c_all[['concept', 'name', 'unit', 'description']]
    # remove the country groups and dont-panic-poverty concepts, which
    # will be process differently.
    trs = trs.iloc[6:-3].copy()
    # create indicator name, unit, description of each concepts.
    trs['key_1'] = 'indicator/'+trs['concept']
    trs['key_2'] = 'unit/'+trs['concept']
    trs['key_3'] = 'description/'+trs['concept']

    trs1 = trs.drop('concept', axis=1).set_index('key_1')
    trs2 = trs.drop('concept', axis=1).set_index('key_2')
    trs3 = trs.drop('concept', axis=1).set_index('key_3')

    name = trs1['name'].fillna("")
    unit = trs2['unit'].fillna("")
    desc = trs3['description'].fillna("")

    for k, v in name.to_dict().items():
        if k not in enj.keys():
            enj.update({k: v})
        else:
            if len(enj[k]) == 0:
                enj.update({k: v})

    for k, v in unit.to_dict().items():
        if k not in enj.keys():
            enj.update({k: v})
        else:
            if len(enj[k]) == 0:
                enj.update({k: v})

    for k, v in desc.to_dict().items():
        if k not in enj.keys():
            enj.update({k: v})
        else:
            if len(enj[k]) == 0:
                enj.update({k: v})

    # menu levels.
    c_all4 = concepts[['concept', 'menu_level1', 'menu_level_2']]

    l1 = c_all4['menu_level1'].unique()
    l2 = c_all4['menu_level_2'].unique()

    for i in l1:
        if i is not np.nan:
            key = to_concept_id(i)
            enj['indicator/'+key] = i

    for i in l2:
        if i is not np.nan:
            key = to_concept_id(i)
            enj['indicator/'+key] = i

    # country groupings
    c5 = c_all[c_all['concept_type'] == 'entity_set'].copy()
    for i, v in c5.iterrows():
        enj['indicator/'+'geo.'+v['concept']] = v['name']

    # manually set the dont-panic-poverty data set concepts.
    # enj['indicator/sg_population'] = enj['indicator/population']
    # enj['indicator/sg_gini'] = enj['indicator/gini']
    # enj['indicator/sg_gdp_p_cap_const_ppp2011_dollar'] = enj['indicator/gdp_p_cap_const_ppp2011_dollar']

    # enj['unit/sg_population'] = enj['unit/population']
    # enj['unit/sg_gini'] = enj['unit/gini']
    # enj['unit/sg_gdp_p_cap_const_ppp2011_dollar'] = enj['unit/gdp_p_cap_const_ppp2011_dollar']

    return enj


def generate_metadata(c_all, concepts, meta2, area, outdir, oneset=False):
    """Generate the metadata.json.

    c_all: ddf--concepts for this repo.
    concepts: graph settings
    meta2: the old metadata.json
    area: area_categorizarion.json
    outdir: the output dir of datapoints.
    oneset: if oneset is true, only one entity set(world_4region) will be added.
    """
    # all measure types.
    mdata = c_all[c_all['concept_type'] == 'measure'][['concept', 'indicator_url', 'scales', 'interpolation']]
    mdata = mdata.set_index('concept')
    mdata = mdata.drop(['longitude', 'latitude'])
    mdata.columns = ['sourceLink', 'scales', 'interpolation']
    mdata['use'] = 'indicator'

    # use OrderedDict in order to keep the order of insertion.
    indb = OrderedDict([['indicatorsDB', {}]])
    mdata_dict = to_dict_dropna(mdata)
    for k in sorted(mdata_dict.keys()):
        indb['indicatorsDB'][k] = mdata_dict.get(k)

    # rm = {'gini': 'sg_gini',
    #       'population': 'sg_population',
    #       'gdp_p_cap_const_ppp2011_dollar': 'sg_gdp_p_cap_const_ppp2011_dollar'
    #       }
    panic_list = ['sg_gini', 'sg_population', 'sg_gdp_p_cap_const_ppp2011_dollar']
    panic = dict([[i, meta2['indicatorsDB'][i]] for i in panic_list])
    indb['indicatorsDB'].update(panic)

    # copy geo and time from old metadata.json
    geo_list = ['geo', 'geo.name', 'geo.latitude', 'geo.longitude',
                'geo.world_4region', 'time']
    for k in geo_list:
        indb['indicatorsDB'][k] = meta2['indicatorsDB'][k]

    if not oneset:
        res = OrderedDict()

        for i in range(len(area)):
            key = to_concept_id(area[i]['n'])
            source = area[i]['sourceName']

            res['geo.'+key] = {'use': 'property', 'scales': ['ordinal'], 'sourceLink': source}
        indb['indicatorsDB'].update(res)

    for i in indb['indicatorsDB'].keys():
        fname = os.path.join(outdir, 'ddf', 'ddf--datapoints--'+i+'--by--geo--time.csv')
        try:
            df = pd.read_csv(fname, dtype={i: float, 'time': int})
        except (OSError, IOError):
            print('no datapoints for ', i)
            continue

        dm = [float(df[i].min()), float(df[i].max())]
        av = [int(df['time'].min()), int(df['time'].max())]

        indb['indicatorsDB'][i].update({'domain': dm, 'availability': av})

    # newdb = OrderedDict([[key, indb['indicatorsDB'][key]] for key in sorted(indb['indicatorsDB'].keys())])
    # indb['indicatorsDB'] = newdb

    # indicator Trees
    indb['indicatorsTree'] = OrderedDict([['id', '_root'], ['children', []]])
    ti = OrderedDict([['id', 'time']])
    pro = OrderedDict([['id', '_properties'], ['children', [{'id': 'geo'}, {'id': 'geo.name'}]]])

    indb['indicatorsTree']['children'].append(ti)
    c_all3 = concepts[['concept', 'menu_level1', 'menu_level_2']].sort_values(['menu_level1', 'menu_level_2'], na_position='first')

    # change nans to something more convenient
    c_all3['menu_level1'] = c_all3['menu_level1'].apply(to_concept_id).fillna('0')
    c_all3['menu_level_2'] = c_all3['menu_level_2'].apply(to_concept_id).fillna('1')

    c_all3 = c_all3.set_index('concept')

    g = c_all3.groupby('menu_level1').groups

    ks = list(sorted(g.keys()))

    # move 'for_advanced_user' to the end of list.
    if 'for_advanced_users' in ks:
        ks.remove('for_advanced_users')
        ks.append('for_advanced_users')

    # loop though all level1 keys, for each key:
    # if key is nan, insert to the _root tree with {'id': concept_name}
    # else, insert {'id': concept_name, 'children': []}
    # then group all concepts with the key as level 1 menu by level 2 menu
    # loop though each level 2 group and do the same insertion logic as above.
    for key in ks:
        if key == '0':  # so it's NaN
            for i in sorted(g[key]):
                indb['indicatorsTree']['children'].append({'id': i})
            # insert _properities entity after the root level menus as requested.
            indb['indicatorsTree']['children'].append(pro)
            if not oneset:
                for i in range(len(area)):
                    key = 'geo.'+to_concept_id(area[i]['n'])
                    indb['indicatorsTree']['children'][-1]['children'].append({'id': key})
            indb['indicatorsTree']['children'][-1]['children'].append({'id': 'geo.world_4region'})
            continue

        od = OrderedDict([['id', key], ['children', []]])
        indb['indicatorsTree']['children'].append(od)

        g2 = c_all3.ix[g[key]].groupby('menu_level_2').groups
        for key2 in sorted(g2.keys()):
            if key2 == '1':  # it's NaN
                for i in sorted(g2[key2]):
                    indb['indicatorsTree']['children'][-1]['children'].append({'id': i})
            else:
                od = OrderedDict([['id', key2], ['children', []]])
                indb['indicatorsTree']['children'][-1]['children'].append(od)
                for i in sorted(g2[key2]):
                    indb['indicatorsTree']['children'][-1]['children'][-1]['children'].append({'id': i})

    return indb
