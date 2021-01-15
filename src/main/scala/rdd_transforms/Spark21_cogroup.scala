package rdd_transforms

import org.apache.spark.sql.SparkSession

object Spark21_cogroup {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext

		val wordPairsRDD = sc.makeRDD(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))
		val wordPairsRDD2 = sc.makeRDD(Array((3,"xx"),(6,"yy"),(2,"zz")))
		//For each key k in this or other, return a resulting RDD that contains a tuple with the list of values for that key in this as well as other.
		//key一定会存在
		val outRDD = wordPairsRDD.cogroup(wordPairsRDD2)
		outRDD.collect.foreach(println)
		/*
		(1,(CompactBuffer(dd),CompactBuffer()))
		(2,(CompactBuffer(bb),CompactBuffer(zz)))
		(3,(CompactBuffer(aa),CompactBuffer(xx)))
		(6,(CompactBuffer(cc),CompactBuffer(yy)))
		**/
	}
}
