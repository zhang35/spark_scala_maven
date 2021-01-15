package rdd_transforms

import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class MyPartitioner (partitions: Int) extends Partitioner{
	override def numPartitions: Int = {
		partitions
	}
	override def getPartition(key: Any): Int = {
		1
	}
}
object Spark12_partitionBy {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext
		val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("d", 4)), 4)
		//非K-V型的rdd是没有partitionBy方法的, 因为getPartition方法需要根据key参数分区
		//val listRDD: RDD[(Int, String)] = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc"), (4, "ddd")), 4)
		listRDD.partitionBy(new MyPartitioner(3)).mapPartitionsWithIndex{
			//参数较多时用模式匹配
			case (num, datas) => {
				datas.map((_, "分区号: " + num))
			}
		}.collect().foreach(println)
		/*
		((a,1),分区号: 1)
		((b,2),分区号: 1)
		((c,3),分区号: 1)
		((d,4),分区号: 1)
		**/
	}
}
