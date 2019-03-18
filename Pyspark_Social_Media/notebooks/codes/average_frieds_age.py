
def parser(line):
    data = line.strip().split(',')
    age = int(data[2])
    num_friends = int(data[3])
    return (age, num_friends)
    


import findspark
findspark.init()
from pyspark import SparkContext, SparkConf  # for configuring and conneting to db
conf = SparkConf().setMaster('local').setAppName('average friends')  # configuration
sc = SparkContext(conf = conf)  # connection to spark
line = sc.textFile('file:///Users/Amin/Dropbox/Career Deveoment/Data Science/PySpark/Pyspark_Social_Media/data/raw/fakefriends.csv')
line_parser = line.map(parser)  # refer to parser function above
friends_counter = line_parser.mapValues(lambda x: (x,1))
# up untill now no calculation is performed
# the following line is an action which makes an acyclic graphs to run/optimize the code
total_friends = friends_counter.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) # returning a tuple (important)
final_results = total_friends.mapValues(lambda x: round(x[0] / x[1], 2)) # average calculation
results = final_results.collect() # getting the results into a list
sorted_results = sorted(results, reverse=True, key = lambda x : x[1])[:10] # top 10 age grop with highet number of friends
for result in sorted_results:
    print(result)