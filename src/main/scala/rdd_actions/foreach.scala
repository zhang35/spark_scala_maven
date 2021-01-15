package rdd_actions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object foreach {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext
		val listRDD : RDD[Int] = sc.makeRDD(1 to 5, 2)

		//collect是将数据都拉到driver上， foreach则是在每个executor上操作, 且没有返回rdd
		//只是在行动
		listRDD.foreach(println(_))
		/*
		1
		3
		2
		4
		5
		**/
		//查看分区信息
		listRDD.glom().foreach(arr => {
			arr.foreach(print)
			println()
		})
		/*
		1324
		5
		*/
	}
}