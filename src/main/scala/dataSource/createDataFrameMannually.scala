package dataSource

import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @ClassName: createDataFrameMannually
 * @Description: 通过自定义Row手动创建DataFrame
 * @Auther: zhangjiaqi
 * @Date: 2020/11/19 10:36
 * @version : V1.0
 **/
object createDataFrameMannually {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val arrayStructData = Seq(
			Row("James", "Java"), Row("James", "C#"),Row("James", "Python"),
			Row("Michael", "Java"),Row("Michael", "PHP"),Row("Michael", "PHP"),
			Row("Robert", "Java"),Row("Robert", "Java"),Row("Robert", "Java"),
			Row("Washington", null)
		)
		val arrayStructSchema = new StructType().add("name", StringType)
				.add("booksInterested", StringType)

		val df = spark.createDataFrame(
			spark.sparkContext.parallelize(arrayStructData),arrayStructSchema)
		df.printSchema()
		df.show(false)
	}
}
