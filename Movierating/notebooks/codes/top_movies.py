
# we need to perform table join between movie names and movie ids
# developing a look up table for movie names and movie ids 
import os
def movie_lut(): # movie look-up-table
    file = os.path.join(os.getcwd(), r'..', r'data\raw\ml-100k\u.item')
    with open(file) as items:
        movie_dict = dict()  # initializing the look up table
        for line in items:
            data = line.strip().split('|')  # the file is pipe delimiated
            movie_id = data[0]
            movie_name = data[1]
            movie_dict[movie_id] = movie_name
    return(movie_dict)

look_up_table = movie_lut()

# accessing pyspark module
import findspark
findspark.init()

# configure and connect to spark context
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster('local[4]').setAppName('TopMovies')
sc = SparkContext(conf = conf)
# broadcast the look up table to all the nodes
boradcasted_lut = sc.broadcast(look_up_table)
# improt the data into RDD
movies = sc.textFile('file:///Users/Amin/Dropbox/Career Deveoment/Data Science/PySpark/Movierating/data/raw/ml-100k/u.data')
# parse the lines
movie_rating = movies.map(lambda x: x.strip().split('\t')[1:3]).map(lambda x: (x[0], (float(x[1]),1))) # appended 1 to simplify averaging
# perform required mapping and reducing
movie_by_rating = movie_rating.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])).filter(lambda x: x[1][1] > 100).mapValues(lambda x: round(x[0] / x[1],3)) # average
sorted_movies = movie_by_rating.sortBy(lambda x: x[1], ascending = False).map(lambda x: (boradcasted_lut.value[x[0]], x[1])) # replacing movie id by movie name
# collect the results
#top_movie = sorted_movies.first() # first sorted element
# output the results
results = sorted_movies.take(10)
print("Movie:" + "\t" + 'Avg Rating')
print('-'*30)
for movie, avgrat in results:
    print(movie, ":\t", avgrat)
    
# closet the context 
sc.stop()