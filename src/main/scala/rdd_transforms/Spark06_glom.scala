package rdd_transforms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Spark06_glom {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext
		val listRDD: RDD[Int] = sc.makeRDD(1 to 16, 4)
		//将一个分区的数据放到一个数组中
		val glomRDD: RDD[Array[Int]] = listRDD.glom()
		glomRDD.collect.foreach(println)
		/**
		[I@56f71edb
		[I@7207cb51
		[I@2a27cb34
		[I@6892cc6f
		**/
		glomRDD.collect.foreach(array=>{
			println(array.mkString(","))
		})
		/*
			1,2,3,4
			5,6,7,8
			9,10,11,12
			13,14,15,16
		**/
	}
}
