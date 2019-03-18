
import findspark
findspark.init()

# configure and connect to spark context
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster('local[4]').setAppName('TopMovies')
sc = SparkContext(conf = conf)
# improt the data into RDD
movies = sc.textFile('file:///Users/Amin/Dropbox/Career Deveoment/Data Science/PySpark/Movierating/data/raw/ml-100k/u.data')
# parse the lines
movie_rating = movies.map(lambda x: x.strip().split('\t')[1:3]).map(lambda x: (x[0], (float(x[1]),1))) # appended 1 to simplify averaging
# perform required mapping and reducing
movie_by_rating = movie_rating.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])).filter(lambda x: x[1][1]>5).mapValues(lambda x: x[0] / x[1]) # average
sorted_movies = movie_by_rating.sortBy(lambda x: x[1], ascending = False)
# collect the results
#top_movie = sorted_movies.first() # first sorted element
# output the results
results = sorted_movies.take(10)
for movie, avgrat in results:
    print(movie, ":\t\t", avgrat)
    
# closet the context 
sc.stop()