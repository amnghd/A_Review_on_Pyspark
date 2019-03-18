
import findspark
findspark.init() # adding pyspark on the sys.path at the run time
from pyspark import SparkConf, SparkContext
# setting up spark
conf = SparkConf().setMaster('local').setAppName('Minimum_temprature')
sc = SparkContext(conf = conf)
# importing the data
lines = sc.textFile('file:///Users/Amin/Dropbox/Career Deveoment/Data Science/PySpark/Temperature data analysis/data/raw/1800.csv')
# defining the text parser
def parser(line):
    try: # while data is in correct format
        data = line.strip().split(',')
        station = data[0]
        measure = data[2]
        temp = float(data[3]) / 10 # conerting temperature to 1 deg celcius
        return station, measure, temp
    except:
        pass
    
# performing the query
parsed_lines = lines.map(parser)
filtered_data = parsed_lines.filter(lambda x: x[1] == 'TMIN').map(lambda x: (x[0],x[2]))
tempBystation = filtered_data.reduceByKey(min)

# getting the results
results = tempBystation.collect()
# reporting the results
for res in results:
    print(res[0], ':\t', res[1])