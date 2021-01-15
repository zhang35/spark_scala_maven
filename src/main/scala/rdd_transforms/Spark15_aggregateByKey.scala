package rdd_transforms

import org.apache.spark.sql.SparkSession

object Spark15_aggregateByKey {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext

		val wordPairsRDD = sc.makeRDD(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
		//(zeroValue:U,[partitioner: Partitioner]) (seqOp: (U, V) => U,combOp: (U, U) => U)		val aggRDD = wordPairsRDD.aggregateByKey(0)(math.max(_,_), _+_)
		//zeroValue： 给每一个分区中的每一个 key 一个初始值；
		//seqOp： 函数用于在每一个分区中用初始值逐步迭代 value；
		//combOp： 函数用于合并每个分区中的结果。
		val aggRDD = wordPairsRDD.aggregateByKey(5)(math.max(_,_), _+_)
		aggRDD.collect.foreach(println)
		/*
		(b,5)
		(a,5)
		(c,13)
		**/
	}
}
