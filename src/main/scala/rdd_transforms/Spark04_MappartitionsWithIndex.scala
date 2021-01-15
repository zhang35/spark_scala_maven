package rdd_transforms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Spark04_MappartitionsWithIndex {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext
		val listRDD: RDD[Int] = sc.makeRDD(1 to 10, 2)
		val indexRDD: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex{
			//参数较多时用模式匹配
			case (num, datas) => {
				datas.map((_, "分区号: " + num))
			}
		}
		indexRDD.collect.foreach(println)
	}
}
