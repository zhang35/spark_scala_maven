package rdd_actions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object countByKey {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext
		val listRDD : RDD[(Int, Int)] = sc.makeRDD(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 3)

		val res = listRDD.countByKey
		println(res)
		/* 
		Map(3 -> 2, 1 -> 3, 2 -> 1)
		**/
	}
}
