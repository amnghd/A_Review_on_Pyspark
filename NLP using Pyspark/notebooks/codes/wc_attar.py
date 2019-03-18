

# finding pyspark
import findspark
findspark.init() # adding pyspark to sys.path
# importing require modules
from pyspark import SparkConf, SparkContext
# importing regex to perform tokenization
import re
# setting up spark environment
conf = SparkConf().setMaster('local[4]').setAppName('WordCounterFarsi')
sc = SparkContext(conf = conf)
# importing the file
lines = sc.binaryFiles('file:///Users/Amin/Dropbox/Career Deveoment/Data Science/PySpark/NLP using Pyspark/data/interim/attar_sample.txt')
# defining the text parser
def parser(line):
    '''
    Performs the parsing of each line of the book.
    Input is a line of text and output is a list of words in that line.'''
    punctuations = re.compile(r'[^\w]+') 
    tokens = punctuations.split(line)
    tokens = [word.decode('utf-8') for word in tokens if word]
    return tokens

def full_line(line):
    '''
    checks if the line has text.'''
    if line:
        return True
    else: False
# perform mapping
filtered_lines = lines.filter(full_line)
tokens_one = filtered_lines\
.flatMap(parser).map(lambda x: (x,1)) # tokenization and counting
# perform reduction
tokens_count = tokens_one.reduceByKey(lambda x, y : x+ y)
# collect the results
results = tokens_count.collect()
# output 20 most frequent words
sorted_alpha = sorted(results, key = lambda x: x[0]) # sorting alphabetically
sorted_results = sorted(sorted_alpha, reverse = True, key = lambda x: x[1]) # sorting according to frequncy
top20 = sorted_results[:20]
for i, wc in enumerate(top20):
    print('Word-{}: {} \t {} times.'.format(i+1, wc[0], wc[1]))