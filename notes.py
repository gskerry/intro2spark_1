
# Intro to Spark training
# https://www.youtube.com/watch?list=PLTPXxbhUt-YWSgAUhrnkyphnh0oKIT8-j&v=VWeWViFCzzg
# From pyspark repl:

# can eval expressions
x = [1,2,3,4]
x

# Hello Word Count

from operator import add

f = sc.textFile("README.md")
# 16/03/09 01:19:21 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 153.6 KB, free 153.6 KB)
# 16/03/09 01:19:21 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 13.9 KB, free 167.5 KB)
# 16/03/09 01:19:21 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on localhost:33873 (size: 13.9 KB, free: 517.4 MB)
# 16/03/09 01:19:21 INFO SparkContext: Created broadcast 0 from textFile at NativeMethodAccessorImpl.java:-2

wc = f.flatMap(lambda x: x.split(' ').map(lambda x: (x, 1))).reduceByKey(add)
# 16/03/09 01:23:03 INFO FileInputFormat: Total input paths to process : 1

wc.saveAsTextFile("wc_out3.txt")
# (MANY) ERRORS
# attempt debug


word_space = f.flatMap(lambda x: x.split(' '))
# >>> word_map
# PythonRDD[9] at RDD at PythonRDD.scala:43

word_space.saveAsTextFile("wordspace.txt")
# sucess

word_map = word_space.map(lambda x: (x, 1)))
# Doh!
word_map = word_space.map(lambda x: (x, 1))
# >>> word_map
# PythonRDD[13] at RDD at PythonRDD.scala:43

word_map.saveAsTextFile("word_map.txt")
# sucess

word_reduce = word_map.reduceByKey(add)
# >>> word_reduce
# PythonRDD[21] at RDD at PythonRDD.scala:43

word_reduce.saveAsTextFile("word_reduce.txt")
# sucess

# There are nice csv and xls export libraries in python.

# Bash... 
# grep Spark word_reduce.txt/part-*
