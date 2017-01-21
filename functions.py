from bs4 import BeautifulSoup
import requests
import re
import pprint
import json
from pymongo import MongoClient
from credentials import MONGO_SERIES_URL
import datetime
import pprint
import csv


def harvest_rs_functions():
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
                function['term'] = cells[0].get_text(' ', strip=True).encode('utf-8').replace('\xc2\x92', "'").lower()
                # scope = cells[1].get_text(strip=True).replace('SCOPE NOTE ', '')
                # function['scope'] = re.sub(r'\s+', ' ', scope)
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
                        subfunc['term'] = child.lower()
                    else:
                        try:
                            subfunc['narrower'].append({'term':child[2:].strip().lower()})
                        except KeyError:
                            subfunc['narrower'] = []
                            subfunc['narrower'].append({'term':child[2:].strip().lower()})
        except AttributeError:
            pass
    return functions


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


def format_series_query(agency):
    if agency['start_date']['date'] and agency['end_date']['date']:
        start_date = datetime.datetime(agency['start_date']['date'].year, 1, 1)
        end_date = datetime.datetime(agency['end_date']['date'].year, 12, 31)
        query = {
            'recording_agencies': 
                {'$elemMatch': 
                    {
                        'identifier': agency['agency_id'], 
                        '$or': [{'end_date.date': {'$gte': start_date}}, {'end_date.date': None}], 
                        '$or': [{'start_date.date': {'$lte': end_date}}, {'start_date.date': None}]
                    }
                }
        }
    elif agency['start_date']['date']:
        start_date = datetime.datetime(agency['start_date']['date'].year, 1, 1)
        query = {'recording_agencies': {'$elemMatch': {'identifier': agency['agency_id'], '$or': [{'end_date.date': {'$gte': start_date}}, {'end_date.date': None}]}}}
    elif agency['end_date']['date']:
        end_date = datetime.datetime(agency['end_date']['date'].year, 12, 31)
        query = {'recording_agencies': {'$elemMatch': {'identifier': agency['agency_id'], '$or': [{'start_date.date': {'$lte': end_date}}, {'start_date.date': None}]}}}
    else:
        query = {'recording_agencies.identifier': agency['agency_id']}
    return query


def check_duplicates():
    '''
    Look to see if agencies are duplicated across levels in function hierarchy.
    '''
    duplicates = 0
    dbclient = MongoClient(MONGO_SERIES_URL)
    db = dbclient.get_default_database()
    for level1 in db.functionhierarchy.find().limit(1):
        print level1['name']
        series = []
        function = db.functions.find_one({'name': level1['name']})
        if 'agencies' in function:
            for agency in function['agencies']:
                query = format_series_query(agency)
                for s in db.series.find(query):
                    if s['identifier'] in series:
                        print '{} Already there'.format(s['identifier'])
                        duplicates += 1
                    else:
                        series.append(s['identifier'])
        if 'narrower' in level1:
            print 'LEVEL 2'
            for level2 in level1['narrower']:
                print level2['name']
                function = db.functions.find({'name': level2['name']})
                if 'agencies' in function:
                    for agency in function['agencies']:
                        query = format_series_query(agency)
                        for s in db.series.find(query):
                            if s['identifier'] in series:
                                print '{} Already there'.format(s['identifier'])
                                duplicates += 1
                            else:
                                series.append(s['identifier'])
            if 'narrower' in level2:
                print 'LEVEL 3'
                for level3 in level2['narrower']:
                    function = db.functions.find({'name': level3})
                    print level3
                    if 'agencies' in function:
                        for agency in function['agencies']:
                            query = format_series_query(agency)
                            for s in db.series.find(query):
                                if s['identifier'] in series:
                                    print '{} Already there'.format(s['identifier'])
                                    duplicates += 1
                                else:
                                    series.append(s['identifier'])
        print '{} series'.format(len(series))
        print '{} duplicates'.format(duplicates)


def harvest_agift3_functions():
    count = 0
    functions = []
    with open('data/agift.js', 'rb') as data_file:
        function = {}
        subf = {}
        data = data_file.read().replace('&#39;', "'")
        for match in re.finditer(r"V([\d_]+) = new WebFXTreeItem\('([\w -]+)'", data):
            count += 1
            id, name = match.groups()
            levels = id.split('_')
            level = len(levels)
            if level == 1:
                if subf:
                    try:
                        function['narrower'].append(subf)
                    except KeyError:
                        function['narrower'] = []
                        function['narrower'].append(subf)
                    subf = {}
                if function:
                    functions.append(function)
                function = {'term': name}
            elif level == 2:
                if subf:
                    try:
                        function['narrower'].append(subf)
                    except KeyError:
                        function['narrower'] = []
                        function['narrower'].append(subf)
                subf = {'term': name}  
            elif level == 3:
                try:
                    subf['narrower'].append({'term': name})
                except KeyError:
                    subf['narrower'] = []
                    subf['narrower'].append({'term': name})
    functions.append(function)
    pprint.pprint(functions)
    print count 
    return functions


def harvest_agift2_functions():
    from agift2 import tree
    count = 0
    functions = []
    function = {}
    subf = {}
    for branch in tree:
        count += 1
        function = {'term': branch[0]}
        if len(branch) > 2:
            function['narrower'] = []
            for twig in branch[2:]:
                count += 1
                subf = {'term': twig[0]}
                if len(twig) > 2:
                    subf['narrower'] = []
                    for leaf in twig[2:]:
                        count += 1
                        subf['narrower'].append({'term': leaf[0]})
                function['narrower'].append(subf)
        functions.append(function)
    pprint.pprint(functions)
    print count
    return functions


def harvest_agift1_functions():
    functions = []
    function = {}
    subf = {}
    count = 0
    with open('data/agift1.csv', 'rb') as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            if row[0]:
                count += 1
                if function:
                    if subf:
                        try:
                            function['narrower'].append(subf)
                        except KeyError:
                            function['narrower'] = []
                            function['narrower'].append(subf)
                        subf = {}
                    functions.append(function)
                function = {'term': row[0]}
            elif row[1]:
                count += 1
                if subf:
                    try:
                        function['narrower'].append(subf)
                    except KeyError:
                        function['narrower'] = []
                        function['narrower'].append(subf)
                subf = {'term': row[1]}
                if row[2]:
                    count += 1
                    subf['narrower'] = []
                    subf['narrower'].append({'term': row[2]})
            elif row[2]:
                count += 1
                subf['narrower'].append({'term': row[2]})
        function['narrower'].append(subf)
        functions.append(function)
        pprint.pprint(functions)
        print count
        return functions


def write_functions(version):
    if version == 'recordsearch':
        functions = harvest_rs_functions()
    elif version == 'agift1':
        functions = harvest_agift1_functions()
    elif version == 'agift2':
        functions = harvest_agift2_functions()
    elif version == 'agift3':
        functions = harvest_agift3_functions()
    with open('data/functions-{}.txt'.format(version), 'wb') as text_file:
        for function in functions:
            print '{}'.format(function['term'].upper())
            text_file.write('{}\n'.format(function['term'].upper()))
            if 'narrower' in function:
                for subf in function['narrower']:
                    print '  - {}'.format(subf['term'].title())
                    text_file.write('  - {}\n'.format(subf['term'].title()))
                    if 'narrower' in subf:
                        for subsubf in subf['narrower']:
                            print '    - {}'.format(subsubf['term'].title())
                            text_file.write('    - {}\n'.format(subsubf['term'].title()))
    with open('data/functions-{}.json'.format(version), 'wb') as json_file:
        json.dump(functions, json_file, indent=4)
