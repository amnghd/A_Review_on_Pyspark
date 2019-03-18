# find pyspark
import findspark
findspark.init()
# configure and get connected to context
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster('local[4]').setAppName('configuration')
sc = SparkContext(conf=conf)
# import the data to RDD
sales = sc.textFile('file:///Users/Amin/Dropbox/Career Deveoment/Data Science/PySpark/simple/customer-orders.csv')

# perform mapping required
id_cost = sales.map(lambda x: x.strip().split(',')).map(lambda x: (str(x[0]), round(float(x[2]),0)))
# perform reduction required
grouped_cost = id_cost.reduceByKey(lambda x,y: x+y).sortByKey()
sorted_by_values = grouped_cost.map(lambda x : (x[1],x[0])).sortByKey().map(lambda x : (x[1],x[0]))
# collect the results
results = sorted_by_values.collect()
# output the results.
print("ID:\t\tSALES")
for res in results:
	print(res[0], ":\t\t", res[1])