package rdd_transforms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Spark13_groupByKey {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext

		val words = Array("one", "two", "two", "three", "three", "three")
		val wordPairsRDD = sc.makeRDD(words).map(word => (word, 1))
		val group = wordPairsRDD.groupByKey()
		group.collect().foreach(println)
		/*
		(two,CompactBuffer(1, 1))
		(one,CompactBuffer(1))
		(three,CompactBuffer(1, 1, 1))
		**/
		group.map(t=>(t._1, t._2.sum)).collect.foreach(println)
		/*
		(two,2)
		(one,1)
		(three,3)
		**/
	}
}
