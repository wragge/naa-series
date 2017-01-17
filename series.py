from rstools.client import RSSeriesSearchClient, RSSearchClient, TooManyError
from pymongo import MongoClient
import time
import string
import csv
from credentials import MONGO_SERIES_URL

# These are attempts to break up series with more than 20,000 items into smaller chunks.
# I'm dividing them up using a range of prefixes that can be used in wildcard searches for control symbols.
# This can get complicated as often there's a fair bit of variability in the use of control symbols.
C_RANGES = {
    'A1': list(range(190, 194)),
    'A1200': ['L{}'.format(num) for num in range(0, 10)] + ['LE', 'LR', 'R', 1, 3],
    'A1501': ['A{}'.format(num) for num in range(0, 10)] + [1, 'W'],
    'A6135': ['K{}/'.format(num) for num in range(1, 32)] + ['K/'] + list(range(1, 10)),
    'A6770': [letter for letter in string.ascii_uppercase],
    'A9301': ['{}{}'.format(num1, num2) for num2 in range(0, 10) for num1 in range(0, 10)] + [letter for letter in string.ascii_uppercase],
    'A12111': ['1/', '2/', '3/'],
    'B883': ['{}X{}'.format(letter, num) for num in range(0, 10) for letter in ['Q', 'T', 'S', 'W', 'NG', 'D', 'P', 'UK']] + ['{}X{}{}'.format(letter, num1, num2) for num2 in range(0, 10) for num1 in range(0, 10) for letter in ['V', 'N']] + ['{}F'.format(letter) for letter in ['V', 'N', 'Q', 'T', 'S', 'W', 'NG', 'D', 'P', 'UK']] + ['{}G'.format(letter) for letter in ['V', 'Q', 'T', 'S', 'W', 'NG', 'D', 'P', 'UK']] + ['C', 'J'] + ['{}{}'.format(letter, num) for num in range(0, 10) for letter in ['V', 'N', 'Q', 'T', 'S', 'W', 'NG', 'D', 'P', 'UK']] + list(range(0, 10)),
    'B884': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'O', 'P', 'R', 'T', 'U'] + ['{}{}'.format(letter, num) for letter in ['S', 'W'] for num in range(0, 10)] + ['{}{}{}'.format(letter, num1, num2) for letter in ['N', 'Q', 'V'] for num1 in range(0, 10) for num2 in range(0, 10)] + list(range(0, 10)) + ['{}{}'.format(letter1, letter2) for letter1 in ['N', 'Q', 'S', 'V', 'W'] for letter2 in string.ascii_uppercase],
    'B2455': ['A', 'D', 'E', 'F', 'G', 'I', 'J', 'K', 'L', 'N', 'O', 'P', 'Q', 'R', 'T', 'U', 'V', 'X', 'Y', 'Z'] + ['{}{}'.format(letter1, letter2) for letter1 in ['B', 'C', 'H', 'M', 'S', 'W'] for letter2 in string.ascii_uppercase],
    'B4747': [letter for letter in string.ascii_uppercase] + [number for number in range(0, 10)],
    'B6295': ['A'] + [number for number in range(0, 10)],
    'D4878': [letter for letter in string.ascii_uppercase],
    'MP1103/1': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'] + ['PW{}'.format(number) for number in range(0, 10)] + ['PW{}'.format(letter) for letter in string.ascii_uppercase] + [number for number in range(0, 10)] + ['P{}'.format(letter) for letter in string.ascii_uppercase.replace('W', '')] + ['P{}'.format(number) for number in range(0, 10)],
    'MP1103/2': [letter for letter in string.ascii_uppercase] + [number for number in range(0, 10)],
}


class SeriesDetailsHarvester():
    def __init__(self, series_id):
        self.series_id = series_id
        self.total_pages = None
        self.pages_complete = 0
        self.client = RSSeriesSearchClient()
        self.prepare_harvest()
        db = self.get_db()
        self.series = db.series

    def get_db(self):
        dbclient = MongoClient(MONGO_SERIES_URL)
        db = dbclient.get_default_database()
        # items = db.items
        # items.remove()
        return db

    def get_total(self):
        return self.client.total_results

    def prepare_harvest(self):
        self.client.search_series(results_per_page=0, series_id=self.series_id)
        total_results = self.client.total_results
        print '{} series'.format(total_results)
        self.total_pages = (int(total_results) / self.client.results_per_page) + 1
        print self.total_pages

    def start_harvest(self, page=None):
        if not page:
            page = self.pages_complete + 1
        else:
            self.pages_complete = page - 1
        while self.pages_complete < self.total_pages:
            response = self.client.search_series(series_id=self.series_id, page=page, sort='1')
            self.series.insert_many(response['results'])
            self.pages_complete += 1
            page += 1
            print '{} pages complete'.format(self.pages_complete)
            time.sleep(1)


def harvest_all(current):
    for letter in string.ascii_uppercase:
        harvester = SeriesDetailsHarvester(series_id='{}*'.format(letter))
        if harvester.total_pages > 0:
            harvester.start_harvest()


def series_summary(identifier):
    dbclient = MongoClient(MONGO_SERIES_URL)
    db = dbclient.get_default_database()
    series = db.series.find_one({'identifier': identifier})
    print 'SERIES {}'.format(identifier)
    print 'Described: {}'.format(series['items_described']['described_number'])
    print 'Digitised: {}'.format(series['items_digitised'])


def collection_summary():
    dbclient = MongoClient(MONGO_SERIES_URL)
    db = dbclient.get_default_database()
    total = db.series.count()
    described = db.series.find({'items_described.described_number': {'$gt': 0}}).count()
    digitised = db.series.find({'items_digitised': {'$gt': 0}}).count()
    aggregation = [
        {
            '$group': {
                '_id': None,
                'described': {'$sum': '$items_described.described_number'},
                'digitised': {'$sum': '$items_digitised'},
            }
        }
    ]
    result = list(db.series.aggregate(aggregation))
    print '\n    {:40}{}'.format('Total series:', total)
    print '    {:40}{} ({:.1%} of series)'.format('Series with item descriptions:', described, float(described) / total)
    print '    {:40}{} ({:.1%} of series with descriptions)\n'.format('Series with digitised items:', digitised, float(digitised) / described)
    print '    {:40}{}'.format('Total items described:', result[0]['described'])
    print '    {:40}{} ({:.1%} of items described)'.format('Total items digitised:', result[0]['digitised'], float(result[0]['digitised']) / result[0]['described'])


def most_digitised_series():
    dbclient = MongoClient(MONGO_SERIES_URL)
    db = dbclient.get_default_database()
    aggregation = [
        {
            '$group': {
                '_id': '$identifier',
                'described': {'$sum': '$items_described.described_number'},
                'digitised': {'$sum': '$items_digitised'}
            }
        },
        {
            '$sort': {'digitised': -1}
        },
        {
            '$limit': 20
        }
    ]
    result = db.series.aggregate(aggregation)
    total = 0
    print '\n    {:12}Digitised'.format('Series')
    print '    =====================\n'
    for series in list(result):
        print '    {:12}{}'.format(series['_id'], series['digitised'])
        total += series['digitised']
    print '\n    Total       {}'.format(total)


def find_large(prefix):
    dbclient = MongoClient(MONGO_SERIES_URL)
    db = dbclient.get_default_database()
    large = db.series.find({'items_digitised': '20000+', 'identifier': {'$regex': '^{}'.format(prefix)}})
    with open('data/{}-digitised.csv'.format(prefix), 'wb') as csv_file:
        writer = csv.writer(csv_file)
        for series in large:
            print series['identifier']
            writer.writerow([series['identifier'], series['items_digitised']])
    with open('data/{}-access.csv'.format(prefix), 'wb') as csv_file:
        for status in ['OPEN', 'OWE', 'CLOSED', 'NYE']:
            large = db.series.find({'access_status.{}'.format(status): '20000+', 'identifier': {'$regex': '^{}'.format(prefix)}})
            writer = csv.writer(csv_file)
            for series in large:
                writer.writerow([series['identifier'], status, series['access_status'][status]])


def find_all_large():
    dbclient = MongoClient(MONGO_SERIES_URL)
    db = dbclient.get_default_database()
    large = db.series.find({'items_digitised': '20000+'})
    with open('data/large-digitised.csv', 'wb') as csv_file:
        writer = csv.writer(csv_file)
        for series in large:
            print series['identifier']
            writer.writerow([series['identifier'], series['items_digitised']])
    with open('data/large-access.csv', 'wb') as csv_file:
        for status in ['OPEN', 'OWE', 'CLOSED', 'NYE']:
            large = db.series.find({'access_status.{}'.format(status): '20000+'})
            writer = csv.writer(csv_file)
            for series in large:
                writer.writerow([series['identifier'], status, series['access_status'][status]])


def harvest_large_series(identifier, control_range=None, ignore_check=True):
    # First let's check that the defined range will get everything
    if not control_range:
        control_range = [letter for letter in string.ascii_uppercase] + [number for number in range(0, 10)]  # + [p for p in string.punctuation]
    total = 0
    digitised = 0
    access = {}
    dbclient = MongoClient(MONGO_SERIES_URL)
    db = dbclient.get_default_database()
    series = db.series.find_one({'identifier': identifier})
    described = series['items_described']['described_number']
    for control in control_range:
        client = RSSearchClient()
        try:
            client.search(series=identifier, control='{}*'.format(control))
        except TooManyError:
            print '{}: more than 20,000'.format(control)
        else:
            print '{}: {}'.format(control, client.total_results)
            total += int(client.total_results)
    print '{} of {} items found'.format(total, described)
    if total == described:
        print '\nYay! All items found!'
    else:
        print '{} items missing -- need to rework the range?'.format(described - total)
    print '\nNow checking for digitised items...\n'
    for control in control_range:
        client = RSSearchClient()
        client.search(series=identifier, control='{}*'.format(control), digital=['on'])
        print '{}: {}'.format(control, client.total_results)
        try:
            digitised += int(client.total_results)
        except TypeError:
            pass
    print '\nDigitised: {}'.format(digitised)
    print '\nNow checking for access status...\n'
    for control in control_range:
        for status in ['OPEN', 'OWE', 'CLOSED', 'NYE']:
            client.search(series=identifier, control='{}*'.format(control), access=status)
            print '{}: {} -- {}'.format(control, status, client.total_results)
            try:
                access[status] += int(client.total_results)
            except KeyError:
                access[status] = int(client.total_results)
    print '\nAccess status\n'
    for s, t in access.items():
        print '{}: {}'.format(s, t)


def check_totals():
    ''' Check the totals in series details against those returned by item searches.'''
    client = RSSearchClient()
    dbclient = MongoClient(MONGO_SERIES_URL)
    db = dbclient.get_default_database()
    for series in db.series.find({'items_described.described_number': {'$lte': 20000, '$gt': 0}}).batch_size(20):
        client.search(series=series['identifier'])
        if int(client.total_results) != series['items_described']['described_number']:
            print '{}: {} of {}'.format(series['identifier'], client.total_results, series['items_described']['described_number'])
        time.sleep(0.5)


def load_digitised_csv():
    dbclient = MongoClient(MONGO_SERIES_URL)
    db = dbclient.get_default_database()
    with open('data/large-digitised-updated.csv', 'rb') as digitised_csv:
        reader = csv.reader(digitised_csv)
        for row in reader:
            print '{} - {}'.format(row[0], int(row[2]))
            db.series.update_one({'identifier': row[0]}, {'$set': {'items_digitised': int(row[2])}})


def percentage_digitised():
    dbclient = MongoClient(MONGO_SERIES_URL)
    db = dbclient.get_default_database()
    pipeline = [
        {'$match': {'items_described.described_number': {'$gt': 1}}},
        {'$project': {'identifier': 1, 'title': 1, 'items_described.described_number': 1, 'items_digitised': 1, 'percent': {'$multiply': [{'$divide': ['$items_digitised', '$items_described.described_number']}, 100]}}},
        {'$sort': {'percent': -1}}
    ]
    series = db.series.aggregate(pipeline)
    return series


def group_percentages():
    groups = {}
    for group in range(0, 100, 10):
        groups[group] = 0
    series = percentage_digitised()
    for s in series:
        for start in range(0, 100, 10):
            if s['percent'] > start and s['percent'] <= start + 10:
                groups[start] += 1
                break
    print groups


def write_percentages():
    series = percentage_digitised()
    with open('data/series-digitised-percentages.csv', 'wb') as csv_file:
        writer = csv.writer(csv_file)
        for s in series:
            writer.writerow([s['identifier'], s['title'].encode('utf-8'), s['items_described']['described_number'], s['items_digitised'], s['percent']])


def write_counts():
    dbclient = MongoClient(MONGO_SERIES_URL)
    db = dbclient.get_default_database()
    pipeline = [
        {'$match': {'items_described.described_number': {'$gt': 1}}},
        {'$project': {'identifier': 1, 'items_described.described_number': 1, 'items_digitised': 1, 'percent': {'$multiply': [{'$divide': ['$items_digitised', '$items_described.described_number']}, 100]}}},
        {'$group': {'_id': {'described': '$items_described.described_number', 'percent': '$percent'}, 'count': {'$sum': 1}}}
    ]
    series = db.series.aggregate(pipeline)
    with open('data/series-digitised-counts.csv', 'wb') as csv_file:
        writer = csv.writer(csv_file)
        for s in series:
            writer.writerow([s['_id']['described'], s['_id']['percent'], s['count']])


def prefix_counts():
    prefixes = []
    dbclient = MongoClient(MONGO_SERIES_URL)
    db = dbclient.get_default_database()
    with open('data/series_prefixes.csv', 'rb') as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            total = db.series.find({'identifier': {'$regex': '^{}[0-9]+'.format(row[1])}}).count()
            described = db.series.find({'identifier': {'$regex': '^{}[0-9]+'.format(row[1])}, 'items_described.described_number': {'$gt': 0}}).count()
            digitised = db.series.find({'identifier': {'$regex': '^{}[0-9]+'.format(row[1])}, 'items_digitised': {'$gt': 0}}).count()
            pipeline = [
                {
                    '$match': {'identifier': {'$regex': '^{}[0-9]+'.format(row[1])}}
                },
                {
                    '$group': {
                        '_id': None,
                        'described': {'$sum': '$items_described.described_number'},
                        'digitised': {'$sum': '$items_digitised'},
                    }
                }
            ]
            result = list(db.series.aggregate(pipeline))
            try:
                prefixes.append(({'prefix': row[1], 'description': row[0], 'total_series': total, 'series_with_descriptions': described, 'series_with_digitised': digitised, 'total_described': result[0]['described'], 'total_digitised': result[0]['digitised']}))
            except IndexError:
                prefixes.append(({'prefix': row[1], 'description': row[0], 'total_series': 0, 'series_with_descriptions': 0, 'series_with_digitised': 0, 'total_described': 0, 'total_digitised': 0}))
    return prefixes


def write_prefix_counts():
    prefixes = prefix_counts()
    with open('data/series_counts_by_prefix.csv', 'wb') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['prefix', 'description', 'total series', 'series with descriptions', 'series with digitised items', 'total items described', 'total items digitised'])
        for prefix in prefixes:
            print '\n{}: {}'.format(prefix['prefix'], prefix['description'])
            print '    {:40}{}'.format('Total series:', prefix['total_series'])
            print '    {:40}{} ({:.1%} of series)'.format('Series with item descriptions:', prefix['series_with_descriptions'], float(prefix['series_with_descriptions']) / prefix['total_series'] if prefix['total_series'] else 0)
            print '    {:40}{} ({:.1%} of series with descriptions)'.format('Series with digitised items:', prefix['series_with_digitised'], float(prefix['series_with_digitised']) / prefix['series_with_descriptions'] if prefix['series_with_descriptions'] else 0)
            print '    {:40}{}'.format('Total items described:', prefix['total_described'])
            print '    {:40}{} ({:.1%} of items described)'.format('Total items digitised:', prefix['total_digitised'], float(prefix['total_digitised']) / prefix['total_described'] if prefix['total_described'] else 0)
            writer.writerow([prefix['prefix'], prefix['description'], prefix['total_series'], prefix['series_with_descriptions'], prefix['series_with_digitised'], prefix['total_described'], prefix['total_digitised']])


