package rdd_transforms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Spark11_coalesce {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext
		val listRDD: RDD[Int] = sc.makeRDD(Seq(1,2,1,5,2,9,6,1), 4)
		println("缩减分区前分区数", listRDD.getNumPartitions)
		val coRDD: RDD[Int] = listRDD.coalesce(3)
		println("缩减分区后分区数", coRDD.partitions.size)
		/*
		(缩减分区前分区数,4)
		(缩减分区后分区数,3)
		**/
		listRDD.mapPartitionsWithIndex{
			//参数较多时用模式匹配
			case (num, datas) => {
				datas.map((_, "分区号: " + num))
			}
		}.collect().foreach(println)
		/*
		(1,分区号: 0)
		(2,分区号: 0)
		(1,分区号: 1)
		(5,分区号: 1)
		(2,分区号: 2)
		(9,分区号: 2)
		(6,分区号: 3)
		(1,分区号: 3)
		**/
		coRDD.mapPartitionsWithIndex{
			//参数较多时用模式匹配
			case (num, datas) => {
				datas.map((_, "分区号: " + num))
			}
		}.collect().foreach(println)
		/*
		(1,分区号: 0)
		(2,分区号: 0)
		(1,分区号: 1)
		(5,分区号: 1)
		(2,分区号: 2)
		(9,分区号: 2)
		(6,分区号: 2)
		(1,分区号: 2)
		**/
	}
}
