# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import requests
from io import BytesIO
from tomorrow import threads


wdi_path = '../../../ddf--wdi/output/ddf--concepts--continuous.csv'
gd_path = '../output/ddf/ddf--concepts.csv'

@threads(8, timeout=20)
def get_indicator_url(sheet_url):
    if sheet_url is np.nan:
        return np.nan
    if 'gapminder.org' in sheet_url:
        return np.nan
    r = requests.get(sheet_url)
    return r


def get_sheet(r):
    try:
        sheet = pd.read_excel(BytesIO(r.content), sheetname='Settings')
        sheet = sheet.set_index('Indicator-settings in the graph')
        # print(sheet.ix['Source link'].iloc[0])
        return sheet.ix['Source link'].iloc[0]
    except Exception as e:
        print(e.value)
        return np.nan


if __name__ == '__main__':
    wdi = pd.read_csv(wdi_path)
    gd = pd.read_csv(gd_path)

    res = []
    for i, row in gd[['concept', 'indicator_url']].iloc[40:50].iterrows():
        concept = row['concept']
        url = row['indicator_url']
        u = get_indicator_url(url+'&output=xls')
        res.append((concept, u))

    res = dict(res)
    for k, v in res.items():
        res[k] = get_sheet(res[k])

    print(res)
