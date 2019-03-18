
import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from collections import OrderedDict # to save the order that entries are added
import matplotlib.pyplot as plt
conf = SparkConf().setMaster('local').setAppName('Histogram') # configuration
sc = SparkContext(conf = conf)  # Main entry point for Spark

lines = sc.textFile('file:///Users/Amin/Dropbox/Career Deveoment/Data Science/PySpark/Movierating/data/raw/ml-100k/u.data')
# mapping line into rating values
ratings = lines.map(lambda x: x.strip().split('\t')[2])
# reducing based on the provided value (no key is available)
histogram = ratings.countByValue()
ordered_hist = OrderedDict(sorted(histogram.items(), key = lambda t:t[1], reverse=True))

ratings = []
frequencies = []
for rating, frequency in ordered_hist.items():
    print('Rating *{}* has {} entry.'.format(rating, frequency))
    ratings.append(str(rating))
    frequencies.append(frequency)
    
    
plt.bar(ratings, frequencies)
plt.xlabel('Ratings')
plt.ylabel('Frequencies')
plt.show()