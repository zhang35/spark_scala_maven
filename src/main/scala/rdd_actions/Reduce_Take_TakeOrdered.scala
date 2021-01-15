package rdd_actions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Reduce_Take_TakeOrdered {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext
		//分区数若不能整除数据，多余的数据放到最后一个分区里
		val listRDD: RDD[Int] = sc.makeRDD(Array(2, 5, 4, 6, 8, 3), 2)

		val red = listRDD.reduce(_+_)
		println(red)

		/*
		28
		**/

		val take: Array[Int] = listRDD.take(6)
		take.foreach(println)
		/*
		2
		5
		4
		6
		8
		3
		**/
		val takeOrdered: Array[Int] = listRDD.takeOrdered(6)
		takeOrdered.foreach(println)
		/*
		2
		3
		4
		5
		6
		8
		**/
	}
}
