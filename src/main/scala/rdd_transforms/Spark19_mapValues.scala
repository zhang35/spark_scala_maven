package rdd_transforms

import org.apache.spark.sql.SparkSession

object Spark19_mapValues {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext

		val wordPairsRDD = sc.makeRDD(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))

		val outRDD = wordPairsRDD.mapValues(_ + "|||")
		outRDD.collect.foreach(println)
		/*
		(3,aa|||)
		(6,cc|||)
		(2,bb|||)
		(1,dd|||)
		**/
	}
}
