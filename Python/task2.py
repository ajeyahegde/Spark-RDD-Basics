import os
import sys
import json
import time

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark import SparkContext
answer = {}
review_filepath = sys.argv[1]
output_filepath = sys.argv[2]
num_partitions = int(sys.argv[3])
sc = SparkContext()

stat1 = {}
stat2 = {}
startTime = time.time()
rdd = sc.textFile(review_filepath)

data = rdd.map(json.loads)
businessRDD = data.map(lambda x: (x['business_id'], 1))
businessGroup = businessRDD.reduceByKey(lambda a, b: a+b)
top_business = businessGroup.takeOrdered(10, lambda a: -a[1])
exec_time1 = time.time()-startTime
stat1["n_partition"] = rdd.getNumPartitions()
stat1["n_items"] = data.glom().map(len).collect()
stat1["exe_time"] = exec_time1


startTime = time.time()
rdd = sc.textFile(review_filepath, num_partitions)
data = rdd.map(json.loads)
businessRDD = data.map(lambda x: (x['business_id'], 1))
businessGroup = businessRDD.reduceByKey(lambda a, b: a+b)
top_business = businessGroup.takeOrdered(10, lambda a: -a[1])
exec_time2 = time.time()-startTime
stat2["n_partition"] = rdd.getNumPartitions()
stat2["n_items"] = data.glom().map(len).collect()
stat2["exe_time"] = exec_time2
answer={}
answer["default"] = stat1
answer["customized"] = stat2
with open(output_filepath, "w") as outfile:
    json.dump(answer, outfile, indent=5)



