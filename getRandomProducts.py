import random
from pymongo import MongoClient
import csv
import re

client = MongoClient()
db = client.test
LIMIT = 100000
numRecords = 1000
collections = ['amazon', 'flipkart', 'snapdeal']

for i in collections:
    c = db[i]
    regex = re.compile("Clothing")
    clothes = c.find({"record.product_category":regex}).limit(LIMIT)
    results = random.sample(list(clothes), numRecords)
    file_name = 'rand_' + i + '.csv'
    with open (file_name, 'w')  as fp:
        a = csv.writer(fp)
        for row in results:
            a.writerow([row])





        




