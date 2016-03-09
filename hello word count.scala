

val f = sc.textFile("data/README.md")

val words_space = f.flatMap(l => l.split(" "))

words_space.saveAsTextFile("outputs/scala_words_space.txt")

val words_mapped = words_space.map(word => (word, 1))

words_mapped.saveAsTextFile("outputs/scala_words_mapped.txt")

val words_reduced = words_mapped.reduceByKey(_ + _)

words_reduced.saveAsTextFile("outputs/scala_words_reduced.txt")

// $ grep Spark scala_words_reduced.txt/part-*
/*
(SparkPi,2)
(Spark.,1)
(Spark,13)
(Spark](#building-spark).,1)
(Spark"](http://spark.apache.org/docs/latest/building-spark.html).,1)
*/