package rdd_transforms

import org.apache.spark.sql.SparkSession

object Spark14_reduceByKey {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext

		val words = Array("one", "two", "two", "three", "three", "three")
		val wordPairsRDD = sc.makeRDD(words).map(word => (word, 1))
		//reduceByKey相比于groupByKey，在shuffle前有combine(预聚合)操作, 返回结果是RDD[k,v]
		//groupByKey: 按照key分组，直接shuffle
		//reduceByKey一般效率较高
		val reduceRDD = wordPairsRDD.reduceByKey(_+_)
		reduceRDD.collect.foreach(println)
		/*
		(two,2)
		(one,1)
		(three,3)
		**/
	}
}
