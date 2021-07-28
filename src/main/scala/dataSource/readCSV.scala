package dataSource

import org.apache.spark.sql.SparkSession

/**
 * @ClassName: readCSV
 * @Description: TODO
 * @Auther: zhangjiaqi
 * @Date: 2021/7/6 11:20
 * @version : V1.0
 **/
object readCSV {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder()
				.appName("CSV Reader")
				.master("local")
				.getOrCreate()

		val result = spark.read.format("csv")
				.option("delimiter", "|")
				.option("header", "true")
				.option("quote", "'")
				.option("nullValue", "\\N")
				.option("inferSchema", "true")
				.load("E:\\curvedTrajectory.csv")

		result.show()
		result.printSchema()

	}
}
