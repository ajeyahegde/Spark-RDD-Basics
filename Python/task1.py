import os
import sys
import json
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark import SparkContext
review_filepath = sys.argv[1]
output_filepath = sys.argv[2]
answer = {}
sc = SparkContext()
sc.setLogLevel('WARN')

rdd = sc.textFile(review_filepath)
data = rdd.map(json.loads)
#Q1 - Total number of reviews
total_count = data.count()
answer["n_review"] = total_count

#Q2 - Numbers of reviews in 2018
reviewRDD = data.map(lambda x: x['date'])
filteredRDD = reviewRDD.filter(lambda x: x.startswith('2018'))
date_count = filteredRDD.count()
answer["n_review_2018"] = date_count

#Q3 - Number of distinct Users
userRDD = data.map(lambda x: x['user_id'])
num_users = userRDD.distinct().count()
answer["n_user"] = num_users

#Q4 - Top 10 users who wrote largest number of reviews
userRDDUnique = data.map(lambda x: [x['user_id'], 1])
userGroup = userRDDUnique.reduceByKey(lambda a, b: a+b).sortByKey()
answer["top10_user"] = userGroup.takeOrdered(10, lambda a: -a[1])

#Q5 - Distinct number of business
businessRDD = data.map(lambda x: x['business_id']).distinct()
num_business = businessRDD.count()
answer["n_business"] = num_business

#Q6 - Top 10 Business
businessRDDUnique = data.map(lambda x: (x['business_id'], 1))
businessGroup = businessRDDUnique.reduceByKey(lambda a, b: a+b).sortByKey()
answer["top10_business"] = businessGroup.takeOrdered(10, lambda a: -a[1])

with open(output_filepath, "w") as outfile:
    json.dump(answer, outfile, indent=5)