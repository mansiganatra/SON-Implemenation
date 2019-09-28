from pyspark import SparkContext, StorageLevel
import sys
import json
import csv
import itertools
from time import time
import math


result = {}
SparkContext.setSystemProperty('spark.executor.memory', '4g')
SparkContext.setSystemProperty('spark.driver.memory', '4g')
sc = SparkContext('local[*]', 'task2')

reviews_input_file_path = "./review.json"
business_input_file_path = "./business.json"
user_business_csv = "./user_business.csv"
output_file_path = "./task2_result.json"
supportThreshold = 4
case = 1

reviewsRDD = sc.textFile(reviews_input_file_path)\
    .map(lambda x: json.loads(x))\
    .map(lambda entry: (entry['business_id'], entry['user_id']))

businessRDD = sc.textFile(business_input_file_path)\
    .map(lambda x: json.loads(x)).map(lambda entry: (entry['business_id'], entry['state']))\
    .filter(lambda entry: entry[1] == 'NV')

start = time()
user_business_list = businessRDD.join(reviewsRDD)\
    .map(lambda entry: (entry[1][1],entry[0]))\
    .collect()

print(user_business_list)

with open(user_business_csv, 'w+', encoding="utf-8") as fp:
    fp.write("user_id,business_id")
    fp.write('\n')
    fp.write('\n'.join('{},{}'.format(x[0],x[1]) for x in user_business_list))
