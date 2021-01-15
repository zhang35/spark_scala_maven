package rdd_transforms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Spark08_filter {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext
		val listRDD: RDD[Int] = sc.makeRDD(1 to 16, 4)
		val filterRDD: RDD[Int] = listRDD.filter(_ % 2 == 0)
		filterRDD.collect.foreach(println)
		/*
			2
			4
			6
			8
			10
			12
			14
			16
		**/
	}
}
