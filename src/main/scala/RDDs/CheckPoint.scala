package RDDs

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @ClassName: addRowsToRDD
 * @Description: TODO
 * @Auther: zhangjiaqi
 * @Date: 2021/1/13 19:43
 * @version : V1.0
 **/
object CheckPoint {

	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext
		//设定检查点保存目录
		sc.setCheckpointDir("cp")

		val rdd = sc.makeRDD(1 to 4, 2)
		val mapRDD = rdd.map((_, 1))

		val reduceRDD = mapRDD.reduceByKey(_+_)

		//在cp目录中生成了备份文件，
		reduceRDD.checkpoint()

		println(reduceRDD.toDebugString)

		//加入执行操作，才能真正执行以上操作
		reduceRDD.collect.foreach(println)
		/*
		checkPoint前
		(2) ShuffledRDD[2] at reduceByKey at CheckPoint.scala:24 []
		 +-(2) MapPartitionsRDD[1] at map at CheckPoint.scala:23 []
			|  ParallelCollectionRDD[0] at makeRDD at CheckPoint.scala:22 []
		**/
		sc.stop()
	}
}
