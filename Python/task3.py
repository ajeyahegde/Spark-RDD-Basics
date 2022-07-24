import os
import sys
import json
import time

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark import SparkContext
answer = {}
review_filepath = sys.argv[1]
business_filepath = sys.argv[2]
output1_filepath = sys.argv[3]
output2_filepath = sys.argv[4]
sc = SparkContext()
startTime = time.time()
review_file= sc.textFile(review_filepath)
review_rdd = review_file.map(json.loads)
business_file = sc.textFile(business_filepath)
business_rdd = business_file.map(json.loads)

business_stars = review_rdd.map(lambda x: (x['business_id'], x['stars']))
business_city = business_rdd.map(lambda x: (x['business_id'], x['city']))
business_join = business_stars.join(business_city)
business = business_join.map(lambda x: (x[1][1], x[1][0]))
business_sorted = business.groupByKey().mapValues(lambda x: sum(x) / len(x)).sortByKey()
endTime = time.time()
total = endTime-startTime

startTime = time.time()
cities = business_sorted.collect()
sorted(cities, key=lambda x: x[1], reverse=True)
for i in range(10):
    print(cities[i])
endTime = time.time()
task1Time = total+endTime-startTime

startTime = time.time()
print(business_sorted.takeOrdered(10, lambda a: -a[1]))
endTime = time.time()
task2Time = total+endTime-startTime

list = rdd5.takeOrdered(rdd5.count(), lambda a: -a[1])
file = open(output1_filepath, 'w')
file.write("city,stars\n")
for item in list:
    file.write(str(item[0])+","+str(item[1])+"\n")
file.close()

answer["m1"] = task1Time
answer["m2"] = task2Time
answer["reason"] = ""
with open(output2_filepath, "w") as outfile:
    json.dump(answer, outfile, indent=5)
