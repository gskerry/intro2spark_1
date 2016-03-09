
val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
// format: java.text.SimpleDateFormat = java.text.SimpleDateFormat@f67a0200

case class Register (d: java.util.Date, uuid: String, cust_id: String, lat: Float, lng: Float)
// defined class Register
/*
scala> Register
res0: Register.type = Register
*/

case class Click (d: java.util.Date, uuid: String, landing_page: Int)
// defined class Click

/*
val reg = sc.textFile("data/reg.tsv").map(_.split("\t")).map(
	r => (r(1), Register(format.parse(r(0)), r(1), r(2), r(3).toFloat, r(4).toFloat))
)
*/

val register_RDD = sc.textFile("data/reg.tsv")
// (!) remember RDDs are automatically splicing up by rows

register_RDD.saveAsTextFile("outputs/join_register_RDD.txt")
// not much to distinguish it from original data file... just now in partition

val register_tuples = register_RDD.map(_.split("\t"))
// split up by tabs
// now have tuples for each row

register_tuples.saveAsTextFile("outputs/join_register_tuples.txt")
// not really a sensical output... 

val register_keyvals = register_tuples.map(
	r => (r(1), Register(format.parse(r(0)), r(1), r(2), r(3).toFloat, r(4).toFloat))
)
// create key vals for each tuple following structure of Register class
// format.parse... date string to timestamp

register_keyvals.saveAsTextFile("outputs/join_register_keyvals.txt")


val clicks = sc.textFile("data/clk.tsv").map(_.split("\t")).map(
	c => (c(1), Click(format.parse(c(0)), c(1), c(2).trim.toInt))
)

clicks.saveAsTextFile("outputs/join_clicks_keyvals.txt")

/* Perform spark JOIN */
register_keyvals.join(clicks).take(1) // look at 1 result
// res2: Array[(String, (Register, Click))] = Array((81da510acc4111e387f3600308919594,(Register(Tue Mar 04 00:00:00 UTC 2014,81da510acc4111e387f3600308919594,2,33.85701,-117.85574),Click(Thu Mar 06 00:00:00 UTC 2014,81da510acc4111e387f3600308919594,61))))

register_keyvals.join(clicks).take(2) // look at 2 results
register_keyvals.join(clicks).collect() // console all results

// N.B. default is an inner_join

register_keyvals.join(clicks).toDebugString
/*
res3: String =
(1) MapPartitionsRDD[15] at join at <console>:42 []
 |  MapPartitionsRDD[14] at join at <console>:42 []
 |  CoGroupedRDD[13] at join at <console>:42 []
 +-(1) MapPartitionsRDD[3] at map at <console>:35 []
 |  |  MapPartitionsRDD[2] at map at <console>:29 []
 |  |  MapPartitionsRDD[1] at textFile at <console>:27 []
 |  |  data/reg.tsv HadoopRDD[0] at textFile at <console>:27 []
 +-(1) MapPartitionsRDD[8] at map at <console>:31 []
    |  MapPartitionsRDD[7] at map at <console>:31 []
    |  MapPartitionsRDD[6] at textFile at <console>:31 []
    |  data/clk.tsv HadoopRDD[5] at textFile at <console>:31 []
*/
// see map of transform/ETL/business logic
// logic level "LAZY EVALUATION"
// not showing cache plan, etc.
// SparkSQL is a different animal

kv.join(clicks).toDebugString







