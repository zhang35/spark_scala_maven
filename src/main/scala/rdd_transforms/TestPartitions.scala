package rdd_transforms

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @ClassName: TestPartitions
 * @Description: TODO
 * @Auther: zhangjiaqi
 * @Date: 2021/1/11 16:13
 * @version : V1.0
 **/
object TestPartitions {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext

		val x = (1 to 10).toList
		import spark.implicits._
		val numbersDf:DataFrame = x.toDF("number")
		println(numbersDf.rdd.partitions.size)
//		numbersDf.write.format("csv").save("E:/testFiles/numbers")

		val people = List(
			(10, "blue"),
			(13, "red"),
			(15, "blue"),
			(99, "red"),
			(67, "blue")
		)
		val peopleDf = people.toDF("age", "color")
		val colorDf = peopleDf.repartition($"color")
		colorDf.show
	}
}
