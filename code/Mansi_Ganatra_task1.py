from pyspark import SparkContext, StorageLevel
import sys
import json
import csv
import itertools
from time import time
import math

if len(sys.argv) != 5:
    print("Usage: ./bin/spark-submit Mansi_Ganatra_task1.py <case	number>	<support> <input_file_path> <output_file_path>")
    exit(-1)
else:
    case = int(sys.argv[1])
    supportThreshold = int(sys.argv[2])
    input_file_path = sys.argv[3]
    output_file_path = sys.argv[4]

def process(entry):
    revisedEntries= entry[0].replace('\'', '').split(',')
    return (revisedEntries[0], revisedEntries[1])

def convertValuesToTuple(entrySet):
    newEntrySet = []
    for entry in entrySet:
        newEntrySet += [(entry, 1)]
    return newEntrySet


def generate_k_candidates(k, current_candidate_set):

    new_k_candidate_set = set()

    # print("Current Candidate set in loop:")
    # print(current_candidate_set)
    current_candidate_list_flattened = frozenset(itertools.chain.from_iterable(current_candidate_set))
    # print("Flattened set:")
    # print(str(current_candidate_list_flattened))

    new_candidate = frozenset()
    for old_candidate in current_candidate_set:
        # print("old candidate: " + str(old_candidate))
        for single_item in current_candidate_list_flattened:
            # print("new single item: " + str(single_item))
            if single_item not in old_candidate:
                new_candidate = frozenset(sorted(old_candidate.union(frozenset([single_item]))))
                # print("new candidate: " + str(new_candidate))
                if len(new_candidate) == k:
                    k_minus_one_subsets = itertools.combinations(new_candidate, k-1)
                    # print("k-1 subsets: ")
                    # for subset in k_minus_one_subsets:
                    #     print(str(subset))
                    is_valid_candidate = True

                    for subset in k_minus_one_subsets:
                        subset_frozen = frozenset(subset)
                        # print("current subset: " + str(subset_frozen))
                        # print(subset_frozen in current_candidate_set)
                        if not subset_frozen in current_candidate_set:
                            # print("invalid")
                            is_valid_candidate = False
                            break

                    if is_valid_candidate:
                        new_k_candidate_set.add(new_candidate)

    # print("Current Candidates in loop:")
    # for cd in new_k_candidate_set:
    #     print(cd)
    new_k_candidate_set = frozenset(sorted(new_k_candidate_set))
    return new_k_candidate_set


def check_and_generate_frequent_k_candidates(original_baskets, current_candidate_set, partition_support_threshold):
    # print("inside check: ")

    # print(str(original_baskets))
    # print(str(current_candidate_set))
    # for i in current_candidate_set:
    #     print(i)
    # # print("********************************************************")
    # for key, val in original_baskets:
    #     print(str(val))

    current_k_frequent_candidates = {}
    current_k_frequents = set()

    for key, values in original_baskets.items():
        # print("inside for loop:")
        # print(type(values))
        # basket_value_set = frozenset([values])
        # print("current basket item: " + str(values))
        for candidate in current_candidate_set:
            # print("current frequent candidate: " + str(candidate))
            if candidate.issubset(values):
                # print("isSubset: " + "true")
                if candidate in current_k_frequent_candidates.keys():
                    current_k_frequent_candidates[candidate] += 1
                else:
                    current_k_frequent_candidates.update({candidate:1})
                    # current_k_frequent_candidates.update({1:candidate})

    # print(":::::::::::::::: frequent candidates :::::::::::::::::::::::::::")
    for key, value in current_k_frequent_candidates.items():
        # print(key)
        # print(value)
        if value >= partition_support_threshold:
            current_k_frequents.add(key)
    # print("::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
    current_k_frequents = frozenset(sorted(current_k_frequents))
    return current_k_frequents



def apriori_implementation(baskets, support_threshold, total_baskets_count):
    # print("********************************************************")
    # for key, values in baskets:
    #     print(values)
    # print("********************************************************")
    # Calculate singletons


    original_baskets = {}
    for key, values in baskets:
        original_baskets.update({key:frozenset(values)})

    partition_support_threshold = math.ceil((float(len(original_baskets))/total_baskets_count) * support_threshold)

    # print("Current parition threshold: " + str(partition_support_threshold))
    # print(original_baskets)
    all_frequent_items_set ={}
    # calculate singletons
    single_frequent_items_candidates = {}
    single_frequent_items = set()
    for key, values in original_baskets.items():
        # print("inside for loop for baskets: ")
        # print(values)
        for val in values:
            if val in single_frequent_items_candidates.keys():
                single_frequent_items_candidates[val] += 1
            else:
                single_frequent_items_candidates.update({val:1})

    for key, value in single_frequent_items_candidates.items():
        if value >= partition_support_threshold:
            single_frequent_items.add(frozenset([key]))

    single_frequent_items = frozenset(sorted(single_frequent_items))
    # print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$0$$$$$$$$$$$$$")
    # for fi in single_frequent_items:
    #     print(fi)

    all_frequent_items_set.update({1:single_frequent_items})

    # print(type(all_frequent_items_set))
    # print(all_frequent_items_set)

    # current_candidate_set =[]
    current_candidate_set = single_frequent_items
    current_frequent_items = set()
    # calculate all other frequent_item_sets starting from k=2

    k=2
    # print(current_candidate_list)
    while len(current_candidate_set) != 0 :
        # print("inside while")
        # print("Current k: " + str(k))
        current_candidate_set = generate_k_candidates(k, current_candidate_set)
        # print("Current Candidate set: ")
        # print(str(current_candidate_set))
        # print("Current candidate length: " + str(len(current_candidate_set)))
        # for i in current_candidate_list:
        #     print(i)
        current_frequent_items = check_and_generate_frequent_k_candidates(original_baskets, current_candidate_set, partition_support_threshold)
        # print("Current Frequent set: ")
        # print(str(current_frequent_items))
        # print("Current frequent length: " + str(len(current_frequent_items)))

        if len(current_frequent_items) != 0:
            all_frequent_items_set.update({k:current_frequent_items})

        k += 1
        current_candidate_set = current_frequent_items

    # print(all_frequent_items_set)

    # print(":::::::::::::::::::::::::::::::::::::::::")
    # print(type(all_frequent_items_set.values()))
    # print(list(all_frequent_items_set.values()))

    with open(output_file_path, "w+") as op:
        op.write("Candidates: " + '\n\n')
        # print(type(all_frequent_items_set.values()))
        for key, itemset in all_frequent_items_set.items():
            # print(type(itemset))
            # print(itemset)
            values = sorted([tuple(sorted(i)) for i in itemset])
            length = len(values)
            for index, tuple_to_write in enumerate(values):
                tuple_to_write_string = ''
                # tuple_to_write = tuple(sorted(value))
                if key == 1:
                    tuple_to_write_string = '(\'' + tuple_to_write[0] + '\')'
                else:
                    tuple_to_write_string = str(tuple_to_write)

                if index != length - 1:
                    tuple_to_write_string += ','

                op.write(tuple_to_write_string)

            op.write("\n\n")

    return list(all_frequent_items_set.values())


def count_frequent_candidates(basket, candidate_list):

    frequent_candidate_counts = []
    # print("Candidate list:")
    # print(candidate_list)
    # candidate_list = candidate_list
    # print(":::::::::::::::::::::::::::::::::::::::")
    # print(type(basket))
    # print(basket)
    for candidate in candidate_list:
        # print(type(candidate))
        # print(candidate)
        if candidate.issubset(basket):
            # print("valid")
            frequent_candidate_counts.append((candidate, 1))

    return frequent_candidate_counts


def son_implementation(basketsRdd, support_threshold, total_baskets_count):

    # num_partitions = basketsRdd.getNumPartitions()
    # partition_st = support_threshold/num_partitions

    map_task_1 = basketsRdd\
        .mapPartitions(lambda entrysets: apriori_implementation(entrysets, support_threshold, total_baskets_count))\
        .map(lambda entry: (entry, 1))
    # print(map_task_1)
    reduce_task_1 = map_task_1.reduceByKey(lambda x, y: x+y)\
        .map(lambda entry: entry[0])

    task1_candidates = reduce_task_1.collect()

    task1_candidates_broadcasted = sc.broadcast(task1_candidates).value
    task1_candidates_broadcasted = frozenset(itertools.chain.from_iterable(task1_candidates_broadcasted))

    map_task_2 = basketsRdd.flatMap(lambda entry: count_frequent_candidates(entry[1], task1_candidates_broadcasted))

    # print(map_task_2.collect())

    reduce_task_2 = map_task_2.reduceByKey(lambda x, y: x+y)

    # print(reduce_task_2.collect())
    frequent_itemsets = reduce_task_2.filter(lambda entry: entry[1] >= supportThreshold)\
        .map(lambda entry: (len(entry[0]), frozenset([entry[0]])))\
        .reduceByKey(lambda set1, set2: set1.union(set2)).sortByKey().collect()\
        # .flatMap(lambda entry: entry[1])\
        # .collect()

    # print("Final Frequent Itemsets: ")
    # print(frequent_itemsets)
    # print(type(frequent_itemsets[1]))

    with open(output_file_path, "a+") as op:
        op.write("Frequent Itemsets: " + '\n\n')
        # print(type(frequent_itemsets))
        for itemset in frequent_itemsets:
            # print(itemset)
            # print(type(itemset[0]))
            values = sorted([tuple(sorted(i)) for i in itemset[1]])
            length = len(values)
            for index, tuple_to_write in enumerate(values):
                tuple_to_write_string = ''
                # print(itemset[0])
                # tuple_to_write = tuple(sorted(value))
                if itemset[0] == 1:
                    tuple_to_write_string = '(\'' + tuple_to_write[0] + '\')'
                else:
                    tuple_to_write_string = str(tuple_to_write)

                if index != length - 1:
                    tuple_to_write_string += ','

                op.write(tuple_to_write_string)

            op.write("\n\n")

    return frequent_itemsets


result = {}
SparkContext.setSystemProperty('spark.executor.memory', '4g')
SparkContext.setSystemProperty('spark.driver.memory', '4g')
sc = SparkContext('local[*]', 'task1')

# input_file_path = "./small2.csv"
# output_file_path = "./task1__2_7_new_result.json"
# supportThreshold = 7
# case=1

start = time()
small1Rdd = sc.textFile(input_file_path).map(lambda entry: entry.split('\n')).map(lambda entry: process(entry))
headers = small1Rdd.take(1)
finalRdd = small1Rdd.filter(lambda entry: entry[0] != headers[0][0])

if case == 1:
    finalRdd = finalRdd.map(lambda entry: (entry[0], entry[1])).groupByKey().mapValues(lambda entry: set(entry))
elif case == 2:
    finalRdd = finalRdd.map(lambda entry: (entry[1], entry[0])).groupByKey().mapValues(lambda entry: set(entry))

# baskets = finalRdd.collect()

total_baskets_count = finalRdd.count()

# with open("./baskests2.json", "w+") as f:
#     for item in baskets:
#         f.write(str(item))
#         f.write("\n")
#     f.close()

results = son_implementation(finalRdd, supportThreshold, total_baskets_count)

# with open(output_file_path, "w+") as op:
#      op.write(str(results))
end = time()
print("Duration: " + str(end-start))
print(total_baskets_count)