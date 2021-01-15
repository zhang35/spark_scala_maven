package rdd_transforms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Spark07_groupBy {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext
		val listRDD: RDD[Int] = sc.makeRDD(1 to 16, 4)
		//按照参数值分组, 即_ % 2, 形成对偶元组(K-V), K是key，V是分组集合
		val groupByRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(_ % 2)
		groupByRDD.collect.foreach(println)
		//分组后的数据，前面的0、1就是key值
		/*
		(0,CompactBuffer(2, 4, 6, 8, 10, 12, 14, 16))
		(1,CompactBuffer(1, 3, 5, 7, 9, 11, 13, 15))
		**/
	}
}
