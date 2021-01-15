package rdd_transforms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Spark10_distinct {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext
		val listRDD: RDD[Int] = sc.makeRDD(Seq(1,2,1,5,2,9,6,1), 4)
		val distinctRDD: RDD[Int] = listRDD.distinct()
		distinctRDD.collect.foreach(println)
		//将rdd中一个分区的数据打乱重组到其它不同分区的算子, 成为shuffle操作
		//distinct虽然是转换算子，但也有可能进行shuffle
		/*
			1
			9
			5
			6
			2
		**/

		//可以指定用几个分区保存结果
		val distinctRDD2: RDD[Int] = listRDD.distinct(2)
		distinctRDD2.collect.foreach(println)
	}
}
