from pyspark import SparkConf , SparkContext
import collections 

conf = SparkConf().setMaster("local").setAppName("ratingHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("C://Users//erfan//Desktop//spark-course//ml-100k//u.data")
ratings = lines.map(lambda x : x.split()[2])
result  = ratings.countByValue()

sortResaults = collections.OrderedDict(sorted(result.items()))
for key , value in sortResaults.items():
    print("%s %i" %(key , value))
