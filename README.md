# SON-Implemenation

Implementation of SON (Savasere, Omiecinski and Navathe: https://dl.acm.org/citation.cfm?id=673300) algorithm using the Apache Spark Framework. The program is developed to find frequent itemsets in two datasets, one simulated dataset (small1.csv, small2.csv) and one real-world dataset generated from Yelp dataset(user_business.csv). Apriori algorithm has been used to generate the frequent itemset.

# Programming Environment
Python: 3.6
Spark: 2.3.2
Scala: 2.11
spark.driver.memory :  4g
spark.executor.memory: 4g

# Hardware Specs:
Model: Lenovo Legion Y530 15
OS: Windows 10, 64-bit
Processor: Intel® Core™ i7-8750H CPU @ 2.20GHz x 6
Memory: 16 GB

# Generating dataset from Yelp Dataset
Run the Mansi_Ganatra_preprocess.py to generate the subset of dataset from yelp dataset in csv format with [userid, businessid] format. The current code generates user, business pairs for Nevada state.# 

# Running the algorithm
# Task1
# Command to run:
spark-submit Mansi_Ganatra_task1.py <case number> <support> <input_file_path> <output_file_path>

# Case 1:
Calculating the combinations of frequent businesses (as singletons, pairs, triples, etc.) that are qualified as frequent given a support threshold. If a business was reviewed more than once by a reviewer, consider this product was rated only once. More specifically, the business ids within each basket are unique. The generated baskets are like:
user1: [business11, business12, business13, ...]
user2: [business21, business22, business23, ...]
user3: [business31, business32, business33, ...]

# Case 2:
Calculating the combinations of frequent users (as singletons, pairs, triples, etc.) that are qualified as frequent given a support threshold. The user ids within each basket are unique. The generated baskets are like:
business1: [user11, user12, user13, ...]
business2: [user21, user22, user23, ...]
business3: [user31, user32, user33, ...]

# Task2
# Command to run:
spark-submit Mansi_Ganatra_task2.py <filter threshold> <support> <input_file_path> <output_file_path>
In this task, the user_business.csv generated in the step above is used. Filtered out qualified users who reviewed more than k businesses. (k is the filter threshold).
The SON algorithm is applied as in case 1 mentioned above for the task 1# .

# Runtime baselines:
# Task 1:

| Input File  | Case | Support | Runtime (sec) |
| ------------- | ------------- | ------------- |  ------------- |
| small2.csv  | 1  | 4 | 11
| small2.csv  | 2 | 9  | 10


# Task 2:
| Input File  | FilterThreshold | Support | Runtime (sec) |
| ------------- | ------------- | ------------- |  ------------- |
| user_business.csv  | 70  | 50 | 115
| small2.csv  | 2 | 9  | 10
