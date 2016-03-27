# -*- coding: utf-8 -*-

import requests
import sys
import os
from io import BytesIO


# github token. change to your token please.
token = 'token ff5cb2a2eeff13be90b22aa9fcc2d6010090212d'

# files:
files = {}

# 1. dont-panic-poverty.csv
org = 'Gapminder'
repo = 'vizabi'
path = '.data/waffles/dont-panic-poverty.csv'
branch = 'develop'
files['dont_panic_poverty'] = {'org': org, 'repo': repo,
                               'branch': branch, 'path': path}

# 2. country entities from systema_globalis
branch = 'newdefinition'
org = 'open-numbers'
repo = 'ddf--gapminder--systema_globalis'
path = 'ddf--entities--geo--country.csv'
files['geo_country'] = {'org': org, 'repo': repo,
                        'branch': branch, 'path': path}

# 3. country_synonyms.xlsx from waffle-server
org = 'Gapminder'
repo = 'waffle-server-importers-exporters'
path = 'data/synonym/country_synonyms.xlsx'
branch = 'world-legacy-with-data'
files['country_synonyms'] = {'org': org, 'repo': repo,
                             'branch': branch, 'path': path}

# 4. area_categorizarion.json from waffle-server
org = 'Gapminder'
repo = 'waffle-server-importers-exporters'
path = 'data/out/gw/meta/area_categorizarion.json'
branch = 'world-legacy-with-data'
files['area_categorizarion'] = {'org': org, 'repo': repo,
                                'branch': branch, 'path': path}

# 5. world_4region entities from systema_globalis
branch = 'newdefinition'
org = 'open-numbers'
repo = 'ddf--gapminder--systema_globalis'
path = 'ddf--entities--geo--world_4region.csv'
files['world_4region'] = {'org': org, 'repo': repo,
                          'branch': branch, 'path': path}

# 6. quantities.json from waffle-server
org = 'Gapminder'
repo = 'waffle-server-importers-exporters'
path = 'data/out/gw/meta/quantities.json'
branch = 'world-legacy-with-data'
files['quantities'] = {'org': org, 'repo': repo,
                       'branch': branch, 'path': path}

# 7. metadata.json from vizabi
org = 'Gapminder'
repo = 'vizabi'
path = '.data/waffles/metadata.json'
branch = 'develop'
files['metadata'] = {'org': org, 'repo': repo,
                     'branch': branch, 'path': path}

# 8. en.json from vizabi
org = 'Gapminder'
repo = 'vizabi'
path = '.data/translation/en.json'
branch = 'develop'
files['en'] = {'org': org, 'repo': repo,
               'branch': branch, 'path': path}


def getGoogleDoc():
    pass


def getFileName(path):
    return path.split('/')[-1]


def getGithubFile(org, repo, branch, path, token, outfile):
    u1 = "https://api.github.com/repos/{org}/{repo}/contents".format(**{'org': org, 'repo': repo})
    r = requests.get(u1, headers={'Authorization': token}, params={'ref': branch})

    for i in r.json():
        if i['name'] == path:
            print(i['sha'])
            sha = i['sha']

    try:
        blob_url = 'https://api.github.com/repos/{org}/{repo}/git/blobs/{sha}'.format(org=org, repo=repo, sha=sha)
    except UnboundLocalError:
        print(r)
        print(org, repo, branch, path)
        raise

    r2 = requests.get(blob_url, headers={'Authorization': token, 'Accept': 'application/vnd.github.v3.raw'})

    cont = BytesIO(r2.content)

    with open(outfile, 'wb') as f:
        f.write(cont.read())
        f.close()

    return


if __name__ == '__main__':
    for v in files.values():
        fn = getFileName(v['path'])
        outfile = os.path.join('../output/tmp', fn)
        print(fn)
        try:
            getGithubFile(v['org'], v['repo'], v['branch'], v['path'], token, outfile)
        except:
            continue
