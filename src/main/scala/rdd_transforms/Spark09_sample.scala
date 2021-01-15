package rdd_transforms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Spark09_sample {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext
		val listRDD: RDD[Int] = sc.makeRDD(1 to 16, 4)
		//withReplacement: 抽样数据是否放回;
		//fraction: 采样百分比。其实是一个分值标准，大于该标准的留下来。
		//seed: 产生随机数。其实是对每个数据打分。
		val sampleRDD: RDD[Int] = listRDD.sample(true, 4, System.currentTimeMillis())
//		val sampleRDD: RDD[Int] = listRDD.sample(false, 0.4, System.currentTimeMillis())
		sampleRDD.collect.foreach(println)
		/*
			3
			4
			5
			7
			8
			9
			13
		**/
	}
}
