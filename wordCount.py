import findspark
findspark.init()
import random

from pyspark import SparkContext 
# rdd1 = sc.read.text('./datase')


sc = SparkContext('local[*]','customer_orders')

path_to_file = ''
order_rdd = sc.textFile(path_to_file)

order_splitmap = order_rdd.map(lambda x: (int(x.split(',')[0]),(float(x.split(',')[2]),1)))
rdd2 = order_splitmap.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])).map(lambda x:(x[0],x[1][0]/x[1][1]))
result = rdd2.collect()
for a in result:
    print(a)

sc.stop()
