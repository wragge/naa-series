from bs4 import BeautifulSoup
import requests
import re
import pprint
import json
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from credentials import MONGO_SERIES_URL
import datetime
import pprint
import csv
import difflib


def harvest_rs_functions():
    functions = []
    subfunc = {}
    function = {}
    r = requests.get('http://recordsearch.naa.gov.au/manual/Provenance/SummaryCRSThes.htm')
    soup = BeautifulSoup(r.text, 'html5lib')
    for row in soup.find_all('tr'):
        cells = row.find_all('td')
        try:
            if not cells[0].find('p'):
                for para in cells[1].find_all('p'):
                    child = para.get_text(' ', strip=True)
                    child = re.sub(r' ([a-z]{1})\b', r'\1', child) # Some letters at end of words get orphaned -- join them up
                    try:
                        subfunc['narrower'].append({'term':child[2:].strip().lower()})
                    except KeyError:
                        subfunc['narrower'] = []
                        subfunc['narrower'].append({'term':child[2:].strip().lower()})
            elif not cells[0].find('p').string.strip() == 'NT':
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
                    child = re.sub(r' ([a-z]{1})\b', r'\1', child) # Some letters at end of words get orphaned -- join them up
                    if child[:2] not in ['NT', 'BT']:
                        if subfunc:
                            try:
                                function['narrower'].append(subfunc)
                            except KeyError:
                                function['narrower'] = []
                                function['narrower'].append(subfunc)
                        subfunc = {}
                        subfunc['term'] = child.strip().lower()
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
    db.functions.create_index('term', unique=True)
    versions = ['recordsearch', 'agift1', 'agift2', 'agift3']
    for version in versions:
        functions = get_functions(version)
        for function in functions:
            try:
                db.functions.insert_one({'term': function['term'], 'versions': [{'version': version, 'level': 0}]})
            except DuplicateKeyError:
                db.functions.update_one({'term': function['term']}, {'$push': {'versions': {'version': version, 'level': 0}}})
            if 'narrower' in function:
                for subf in function['narrower']:
                    try:
                        db.functions.insert_one({'term': subf['term'], 'versions': [{'version': version, 'level': 1, 'parent': function['term']}]})
                    except DuplicateKeyError:
                        db.functions.update_one({'term': subf['term']}, {'$push': {'versions': {'version': version, 'level': 1, 'parent': function['term']}}})
                    if 'narrower' in subf:
                        for subsubf in subf['narrower']:
                            try:
                                db.functions.insert_one({'term': subsubf['term'], 'versions': [{'version': version, 'level': 2, 'parent': subf['term']}]})
                            except DuplicateKeyError:
                                db.functions.update_one({'term': subsubf['term']}, {'$push': {'versions': {'version': version, 'level': 2, 'parent': subf['term']}}})


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
        print data
        for match in re.finditer(r"V([\d_]+) = new WebFXTreeItem\('([\w -,\']+?)','", data):
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
                function = {'term': name.strip().lower()}
            elif level == 2:
                if subf:
                    try:
                        function['narrower'].append(subf)
                    except KeyError:
                        function['narrower'] = []
                        function['narrower'].append(subf)
                subf = {'term': name.strip().lower()}  
            elif level == 3:
                try:
                    subf['narrower'].append({'term': name.strip().lower()})
                except KeyError:
                    subf['narrower'] = []
                    subf['narrower'].append({'term': name.strip().lower()})
    functions.append(function)
    # pprint.pprint(functions)
    # print count 
    return functions


def harvest_agift2_functions():
    from agift2 import tree
    count = 0
    functions = []
    function = {}
    subf = {}
    for branch in tree:
        count += 1
        function = {'term': branch[0].strip().lower()}
        if len(branch) > 2:
            function['narrower'] = []
            for twig in branch[2:]:
                count += 1
                subf = {'term': twig[0].strip().lower()}
                if len(twig) > 2:
                    subf['narrower'] = []
                    for leaf in twig[2:]:
                        count += 1
                        subf['narrower'].append({'term': leaf[0].strip().lower()})
                function['narrower'].append(subf)
        functions.append(function)
    # pprint.pprint(functions)
    # print count
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
                function = {'term': row[0].strip().lower()}
            elif row[1]:
                count += 1
                if subf:
                    try:
                        function['narrower'].append(subf)
                    except KeyError:
                        function['narrower'] = []
                        function['narrower'].append(subf)
                subf = {'term': row[1].strip().lower()}
                if row[2]:
                    count += 1
                    subf['narrower'] = []
                    subf['narrower'].append({'term': row[2].strip().lower()})
            elif row[2]:
                count += 1
                subf['narrower'].append({'term': row[2].strip().lower()})
        function['narrower'].append(subf)
        functions.append(function)
        # pprint.pprint(functions)
        # print count
        return functions


def get_functions(version):
    if version == 'recordsearch':
        functions = harvest_rs_functions()
    elif version == 'agift1':
        functions = harvest_agift1_functions()
    elif version == 'agift2':
        functions = harvest_agift2_functions()
    elif version == 'agift3':
        functions = harvest_agift3_functions()
    return functions


def write_functions(version):
    functions = get_functions(version)
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
                            print '    -- {}'.format(subsubf['term'].title())
                            text_file.write('    -- {}\n'.format(subsubf['term'].title()))
    with open('data/functions-{}.json'.format(version), 'wb') as json_file:
        json.dump(functions, json_file, indent=4)


def list_functions(version, all=True):
    '''Convert hierarchy to simple list.'''
    flist = []
    functions = get_functions(version)
    for function in functions:
        flist.append(function['term'])
        if all and 'narrower' in function:
            for subfunc in function['narrower']:
                flist.append(subfunc['term'])
                if all and 'narrower' in subfunc:
                    for subsubfunc in subfunc['narrower']:
                        flist.append(subsubfunc['term'])
    return sorted(flist)


def compare_functions(version1, version2):
    list1 = list_functions(version1)
    list2 = list_functions(version2)
    not1 = [function for function in list1 if function not in list2]
    not2 = [function for function in list2 if function not in list1]
    print 'Functions in {}, but not in {}:\n'.format(version1, version2)
    for f in not1:
        print f
    print '\nFunctions in {}, but not in {}:\n'.format(version2, version1)
    for f in not2:
        print f

def make_diffs(version1, version2):
    with open('data/functions-{}.txt'.format(version1), 'rb') as file1:
        functions1 = file1.readlines()
    with open('data/functions-{}.txt'.format(version2), 'rb') as file2:
        functions2 = file2.readlines()
    print functions1
    differ = difflib.HtmlDiff()
    print differ.make_table(functions1, functions2, context=True)


def get_used_functions():
    dbclient = MongoClient(MONGO_SERIES_URL)
    db = dbclient.get_default_database()
    print '| Term | Number of agencies | Included in thesaurus |'
    print '|----|----|----|'
    for function in db.functions.find({'agencies': {'$exists': True}}).sort('term'):
        print '| {:30} | {:4} | {} |'.format(function['term'], len(function['agencies']), ', '.join([version['version'] for version in function['versions']]))
    print db.functions.find({'agencies': {'$exists': True}}).sort('term').count()



