# -*- coding: utf-8 -*-

import pandas as pd
import json
import os
from ddf import (extract_entities_groups, extract_entities_country)
# from . vizabi import *

# source files to be read. More can be found in README in this repo.
# indicator name and their hash names.
idt_f = 'quantities.json'
# groupings, there are 5 ways of grouping of countries in the data set.
gps_f = 'area_categorizarion.json'
# Country properities
geo_f = 'country_synonyms.xlsx'
# Region properities
reg_f = 'regions.json'
# indicator properities
concept_f = 'graph_settings - Indicators.csv'
# old en.json, to be updated
enj_f = 'en.json'
# dont-panic-poverty data points
dpp_f = 'dont-panic-poverty.csv'
# discrete concept list from systema_globalis.
# we only use this file to get the world_4region concept
sgdc_f = 'ddf--concepts--discrete.csv'
# country properities from systema_globalis
geo_sg_f = 'ddf--entities--geo--country.csv'

# datapoint folder
dps_f = 'indicators'


def main(source_dir, out_dir):
    # read files
    idt = pd.read_json(os.path.join(source_dir, idt_f))
    gps = pd.read_json(os.path.join(source_dir, gps_f))
    geo = pd.read_excel(os.path.join(source_dir, geo_f))
    regs = json.load(open(os.path.join(source_dir, reg_f)))
    concepts = pd.read_csv(os.path.join(source_dir, concept_f), encoding='utf8')
    enj = json.load(open(os.path.join(source_dir, enj_f)))
    dpp = pd.read_csv(os.path.join(source_dir, dpp_f))
    sgdc = pd.read_csv(os.path.join(source_dir, sgdc_f))
    geo_sg = pd.read_csv(os.path.join(source_dir, geo_sg_f))

    # build ddf
    # 1. entities for country groupings
    g = extract_entities_groups(regs, gps)

    # 2. entities for countries
    c = extract_entities_country(regs, geo, gps, geo_sg)

    print(c.head())
    print(g[list(g.keys())[1]])


if __name__ == '__main__':
    main('../source/', '../output/')
