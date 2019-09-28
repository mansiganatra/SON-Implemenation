from pyspark import SparkContext, StorageLevel
import sys
import json
import csv
import itertools
from time import time
import math


SparkContext.setSystemProperty('spark.executor.memory', '4g')
SparkContext.setSystemProperty('spark.driver.memory', '4g')
sc = SparkContext('local[*]', 'task2')

business_input_file_path = "./business.json"
frequent_itemsets_path = "./frequent_itemsets.txt"

frequent_itemsets_rdd = sc.textFile(frequent_itemsets_path)\
    .flatMap(lambda entry: entry.split("\n\n"))\
    .filter(lambda entry: entry not in '')\
    .flatMap(lambda entries: entries.split(","))\
    .map(lambda entry: entry.replace('(', '').replace(')', '').replace('\'','').strip())\
    # .distinct()

frequent_itemsets = set(frequent_itemsets_rdd.collect())

# print(frequent_itemsets)


businessRDD = sc.textFile(business_input_file_path)\
    .map(lambda x: json.loads(x)).map(lambda entry: (entry['business_id'], entry['state'], entry['categories'], entry['city']))

top10Categories = businessRDD.map(lambda entry: (entry[2], entry[0]))\
    .filter(lambda entry: entry[1] in frequent_itemsets)\
    .flatMap(lambda entry: (entry[0]).split(','))\
    .map(lambda entry: (entry.strip(), 1))\
    .reduceByKey(lambda x,y: x+y)\
    .sortBy(lambda x: x[1], ascending=False)

print(top10Categories.take(10))

top10Places = businessRDD.map(lambda entry: (entry[1], entry[3], entry[0]))\
    .filter(lambda entry: entry[2] in frequent_itemsets)\
    .map(lambda entry: ((entry[0], entry[1]), 1))\
    .reduceByKey(lambda x,y: x+y)\
    .sortBy(lambda x: x[1], ascending=False)

print(top10Places.take(10))
