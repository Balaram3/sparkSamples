import findspark
findspark.init()
from pyspark import SparkContext


logList = [
    "WARN: Thursday 4 September 0405",
    "WARN: Thursday 4 September 0409",
    "ERROR: Thursday 4 September 0409",
    "ERROR: Thursday 4 September 0409",
    "WARN: Thursday 4 September 0409",
]

sc = SparkContext('local[*]','logLevelCount')
sc.setLogLevel("INFO")
logs_rdd  = sc.parallelize(logList)
pair_rdd = logs_rdd.map(lambda x:(x.split(":")[0],1))
reduced_rdd = pair_rdd.reduceByKey(lambda x,y:x+y)
result  = reduced_rdd.collect()
for i in result:
    print(i)
sc.stop()