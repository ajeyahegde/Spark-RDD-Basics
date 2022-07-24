# Spark-RDD-Basics
Repository for Spark Basic RDD Operations

This repository has code for Spark RDD basics operations which use yelp business and review dataset.

*Task 1 Questions:*

A. The total number of reviews 
B. The number of reviews in 2018 
C. The number of distinct users who wrote reviews 
D. The top 10 users who wrote the largest numbers of reviews and the number of reviews they wrote
E. The number of distinct businesses that have been reviewed 
F. The top 10 businesses that had the largest numbers of reviews and the number of reviews they had

*Task 2 Questions:*

Stats to compare the time taken for operations D and F with default partition and specified partition numbers.

*Task 3 Qeustion:*

A. Find average stars for each city.
B. Stats to compare the time taken for sort operations in Question A with and without Spark.

*To execute the code:*

Python:
1) spark-submit task1.py <review_filepath> <output_filepath>
2) spark-submit task2.py <review_filepath> <output_filepath> <n_partition>
3) spark-submit task3.py <review_filepath> <business_filepath> <output_filepath_a> <output_filepath_b>

Scala:
1) spark-submit hw1.jar <review_filepath> <output_filepath>
2) spark-submit task2 hw1.jar <review_filepath> <output_filepath> <n_partition>
3) spark-submit hw1.jar <review_file> <business_file> <output_filepath_a> <output_filepath_b>
