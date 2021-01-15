package rdd_transforms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Spark03_Mappartitions {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext
		val listRDD: RDD[Int] = sc.makeRDD(1 to 10, 2)
		val indexRDD: RDD[Int] = listRDD.mapPartitions(datas => {
			var dataList = datas.toList
			dataList.sorted.foreach(println)
			datas.map(_*2)
		})
		indexRDD.collect.foreach(println)
	}
}
