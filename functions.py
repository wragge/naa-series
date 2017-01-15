from bs4 import BeautifulSoup
import requests
import re
import pprint
import json
from pymongo import MongoClient
from credentials import MONGO_SERIES_URL


def harvest_functions():
    functions = []
    subfunc = {}
    function = {}
    r = requests.get('http://recordsearch.naa.gov.au/manual/Provenance/SummaryCRSThes.htm')
    soup = BeautifulSoup(r.text, 'html5lib')
    for row in soup.find_all('tr'):
        cells = row.find_all('td')
        try:
            if not cells[0].find('p').string.strip() == 'NT':
                if function:
                    functions.append(function)
                if subfunc:
                    try:
                        function['narrower'].append(subfunc)
                    except KeyError:
                        function['narrower'] = []
                        function['narrower'].append(subfunc)
                    subfunc = {}
                function = {}
                function['name'] = cells[0].get_text(' ', strip=True).encode('utf-8')
                scope = cells[1].get_text(strip=True).replace('SCOPE NOTE ', '')
                function['scope'] = re.sub(r'\s+', ' ', scope)
            else:
                for para in cells[1].find_all('p'):
                    child = para.get_text(' ', strip=True)
                    child = re.sub(r' ([a-z]{1})\b', r'\1', child)
                    if child[:2] not in ['NT', 'BT']:
                        if subfunc:
                            try:
                                function['narrower'].append(subfunc)
                            except KeyError:
                                function['narrower'] = []
                                function['narrower'].append(subfunc)
                        subfunc = {}
                        subfunc['name'] = child
                    else:
                        try:
                            subfunc['narrower'].append(child[2:].strip())
                        except KeyError:
                            subfunc['narrower'] = []
                            subfunc['narrower'].append(child[2:].strip())
        except AttributeError:
            pass
    with open('data/functions.txt', 'wb') as text_file:
        for function in functions:
            print '{}'.format(function['name'])
            text_file.write('{}\n'.format(function['name']))
            if 'narrower' in function:
                for subf in function['narrower']:
                    print '  - {}'.format(subf['name'])
                    text_file.write('  - {}\n'.format(subf['name']))
                    if 'narrower' in subf:
                        for subsubf in subf['narrower']:
                            print '    - {}'.format(subsubf)
                            text_file.write('    - {}\n'.format(subsubf))
    with open('data/functions.json', 'wb') as json_file:
        json.dump(functions, json_file, indent=4)


def load_functions():
    dbclient = MongoClient(MONGO_SERIES_URL)
    db = dbclient.get_default_database()
    with open('data/functions.json', 'rb') as json_file:
        functions = json.load(json_file)
        db.functionhierarchy.insert_many(functions)
        for function in functions:
            db.functions.insert_one({'name': function['name'], 'level': 0})
            if 'narrower' in function:
                for subf in function['narrower']:
                    db.functions.insert_one({'name': subf['name'], 'level': 1, 'parent': function['name']})
                    if 'narrower' in subf:
                        for subsubf in subf['narrower']:
                            db.functions.insert_one({'name': subsubf, 'level': 2, 'parent': subf['name']})


def check_duplicates():
    '''
    Look to see if agencies are duplicated across levels in function hierarchy.
    '''
    series = []
    duplicates = 0
    dbclient = MongoClient(MONGO_SERIES_URL)
    db = dbclient.get_default_database()
    for func in db.functionhierarchy.find():
        pass
