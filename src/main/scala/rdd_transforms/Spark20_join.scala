package rdd_transforms

import org.apache.spark.sql.SparkSession

object Spark20_join {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext

		val wordPairsRDD = sc.makeRDD(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))
		val wordPairsRDD2 = sc.makeRDD(Array((3,"xx"),(6,"yy"),(2,"zz")))
		//Each pair of elements will be returned as a (k, (v1, v2)) tuple
		val outRDD = wordPairsRDD.join(wordPairsRDD2)
		outRDD.collect.foreach(println)
		/*
		(2,(bb,zz))
		(3,(aa,xx))
		(6,(cc,yy))
		**/
	}
}
