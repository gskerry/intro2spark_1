
/*
Intro to Spark training
https://www.youtube.com/watch?list=PLTPXxbhUt-YWSgAUhrnkyphnh0oKIT8-j&v=VWeWViFCzzg

From spark shell:
*/

val data = 1 to 10000
/*
data: scala.collection.immutable.Range.Inclusive = Range(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170..
*/

val distData = sc.parallelize(data)
// distData: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:29

distData.filter( _ < 10).collect()
// res0: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)


/*
Log Mining Example (NOT run)
*/

// base RDD
val lines = sc.textFile("hdfs://...")

// transform RDDs
val errors = line.filter(_.startsWith("ERROR"))
val messages = errors.map(_.split("\t")).map(r => r(1))

messages.toDebugString
// shows a map/listing of processing steps


// action1
message.filter(_.contains("mysql")).count()

// action2
message.filter(_.contains("php")).count()



/*
Hello Word Count
*/

val f = sc.textFile("README.md")
// f: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[3] at textFile at <console>:27

val wc = f.flatMap(l => l.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
// wc: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[10] at reduceByKey at <console>:29

/*
flatmap... 
data is getting pulled in by line from files... each partition creating a list of lists...
but we'd rather have a 'flat' dataset space of keywords
*/
/*
Alternatively written... 
val word_space = f.flatMap(l => l.split(" "))
val word_map = word_space.map(word => (word, 1))
val word_reduce = word_map.reduceByKey(_ + _)
May be advisable in real code... easier to debug
*/

wc.saveAsTextFile("wc_out2.txt")
