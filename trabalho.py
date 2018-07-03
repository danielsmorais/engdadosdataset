# hdfs dfs –put arquivo.txt          lancar arquivo para hdfs
# spark-submit SimpleApp.py          submeter codigo ao hdfs

"""SimpleApp.py"""

from pyspark import SparkContext

sc = SparkContext("local", "SimpleApp")
logData = sc.textFile("hdfs://10.7.40.100:9000/user/engdados/input/Seattle_Traffic_Violations_-_2010.csv")
numAs = logData.filter(lambda s: 'c' in s).count()
numBs = logData.filter(lambda s: 'd' in s).count()
print "\n\nLines with c: % i, lines with d: % i\n" % (numAs, numBs)
