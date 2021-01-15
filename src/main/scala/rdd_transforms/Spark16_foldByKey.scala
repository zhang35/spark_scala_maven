package rdd_transforms

import org.apache.spark.sql.SparkSession

object Spark16_foldByKey {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext

		val wordPairsRDD = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)))
		//aggregateByKey 的简化操作， seqop 和 combop 相同
		//先将zeroValue=2应用于每个V,得到：("A",0+2), ("A",2+2)，即：("A",2), ("A",4)，再将映射函
		//数应用于初始化后的V，最后得到：(A,2+4)，即：(A,6)
		val aggRDD = wordPairsRDD.foldByKey(2)(_+_)
		aggRDD.collect.foreach(println)
		/*
		(A,6)
		(B,7)
		(C,3)
		**/
	}
}
