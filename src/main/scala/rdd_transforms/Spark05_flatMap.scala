package rdd_transforms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Spark05_flatMap {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext
		val listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2), List(3, 4)))
		val flatMapRDD: RDD[Int] = listRDD.flatMap(datas => datas)
		flatMapRDD.collect.foreach(println)
	}
}
