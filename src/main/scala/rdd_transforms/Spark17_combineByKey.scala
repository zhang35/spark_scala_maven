package rdd_transforms

import org.apache.spark.sql.SparkSession

object Spark17_combineByKey {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext

		val wordPairsRDD = sc.makeRDD(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)
		//参数: (createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C)
		//作用: 针对相同 K, 将 V 合并成一个集合
		//将相同 key 对应的值相加，同时记录该 key 出现的次数，放入一个二元组
		val aggRDD = wordPairsRDD
			//createCombiner:分区内每种key调用一次(v)=>(v,1): 将key对应的值映射成一个二元组
			.combineByKey((_, 1),
			//mergeValue:分区内将createCombiner()结果与相同的key对应的值做合并, 元组的第一位与v相加，元组第二位自增1
			(acc:(Int, Int), v)=>(acc._1 + v, acc._2+1),
			//mergeCombiners:acc1:将各个分区间相同的key对应的结果做聚合, 元组的第一位与第二位分别累加
			(acc1:(Int, Int), acc2:(Int, Int))=>(acc1._1 + acc2._1, acc1._2 +acc2._2)
		)
		aggRDD.collect.foreach(println)
		/*
		(b,(286,3))
		(a,(274,3))
		**/
	}
}
