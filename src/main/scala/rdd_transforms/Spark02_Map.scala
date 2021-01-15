package rdd_transforms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Spark02_Map {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext
		//分区数若不能整除数据，多余的数据放到最后一个分区里
		val listRDD: RDD[Int] = sc.makeRDD(1 to 10, 2)

		//所有RDD算子的计算功能，全部由executor执行，即 _ * 2
		val mapRDD: RDD[Int] = listRDD.map(_ * 2)
		mapRDD.collect.foreach(println)
		/*
		2
		4
		6
		8
		10
		12
		14
		16
		18
		20
		**/
		//这里i会被网络传输到每个executor上，传递的对象必须可序列化
		val i = 10
		val mapRDD2: RDD[Int] = listRDD.map(_ * i)
		mapRDD2.collect.foreach(println)
		/*
		10
		20
		30
		40
		50
		60
		70
		80
		90
		100
		**/
	}
}
