# Iteratively load a super large json

from __future__ import division
import ijson
import datetime
import pymongo
import sys

from multiprocessing import Pool

cl = pymongo.MongoClient()
db = cl['delhivery_db']
pkg = db['packages']
pkg.remove({})
print(pkg.find().count())


def flatten(package):
    cur_cen = None
    last_time = None
    records = []

    for status in package.get('s', []):
        new_cen = status.get('sl', None)
        new_tim = datetime.datetime.strptime(status.get('sd', None).get('$date')[:-9], '%Y-%m-%dT%H:%M:%S')

        if not new_cen:
            continue

        if cur_cen == new_cen:
            last_time = new_tim
            continue
        record = {'wbn': package.get('wbn'), 'sl': cur_cen, 'sd': last_time, 'in': 1}
        records.append(record)
        cur_cen = new_cen
        last_time = new_tim
        record = {'wbn': package.get('wbn'), 'sl': cur_cen, 'sd': last_time, 'in': 0}
        records.append(record)

    record = {'wbn': package.get('wbn'), 'sl': cur_cen, 'sd': last_time, 'in': 1}
    records.append(record)
    return records[1:]


f = open('/data/aluri/pkg_20_30_jun_2015.json', 'r')
count = 770578

def fn(package):
    data = flatten(package)
    pkg.insert_many(data)

p = Pool(16)
for i, _ in enumerate(p.imap_unordered(fn, ijson.items(f, 'item'), chunksize=10000), 10000):
    sys.stderr.write('\rDone: {0:%}'.format(i/count))
