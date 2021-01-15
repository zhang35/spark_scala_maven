package rdd_actions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Aggregate_Fold {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext
		//分区数若不能整除数据，多余的数据放到最后一个分区里
		val listRDD: RDD[Int] = sc.makeRDD(Array(2, 5, 4, 6, 8, 3), 2)

		//aggregate 函数将每个分区里面的元素通过 seqOp 和初始值进行聚合，然后用
		//combine 函数将每个分区的结果和初始值(zeroValue)进行 combine 操作。这个函数最终返回
		//的类型不需要和 RDD 中元素类型一致。

		//分区内从10开始相加，分区间再从10开始相加
		//所以多了3个10， 从28变成58
		val res = listRDD.aggregate(10)(_+_, _+_)
		println(res)
		/*
		58
		**/

		//折叠操作， aggregate 的简化操作， seqop 和 combop 一样。
		val res1 = listRDD.fold(0)(_+_)
		println(res1)
		/*
		28
		**/
	}
}
