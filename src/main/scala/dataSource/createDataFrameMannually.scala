package dataSource

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @ClassName: createDataFrameMannually
  * @Description: 通过自定义Row手动创建DataFrame
  * @author: zhangjiaqi
  * @Date: 2020/11/19 10:36
  * @version : V1.0
  **/
object createDataFrameMannually {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()

        //方法一：使用createDataFrame
		val arrayStructData = Seq(
			Row("James", "Java"), Row("James", "C#"),Row("James", "Python"),
			Row("Michael", "Java"),Row("Michael", "PHP"),Row("Michael", "PHP"),
			Row("Robert", "Java"),Row("Robert", "Java"),Row("Robert", "Java"),
			Row("Washington", null)
		)
		//创建StructType空对象，用add添加filed
//		val arrayStructSchema = new StructType()
//			.add("name", StringType, false)
//			.add("booksInterested", StringType, true)

		//或创建StructType时传入List StructField作为参数
        //StructField(name, dataType, nullable=True, metadata=None)

		val arrayStructSchema = StructType(List(
            StructField("name", StringType, false),
            StructField("booksInterested", StringType, true)
        ))

		val df = spark.createDataFrame(
			spark.sparkContext.parallelize(arrayStructData),arrayStructSchema)
		df.printSchema()
		df.show(false)

		/*
root
 |-- name: string (nullable = false)
 |-- booksInterested: string (nullable = true)

+----------+---------------+
|name      |booksInterested|
+----------+---------------+
|James     |Java           |
|James     |C#             |
|James     |Python         |
|Michael   |Java           |
|Michael   |PHP            |
|Michael   |PHP            |
|Robert    |Java           |
|Robert    |Java           |
|Robert    |Java           |
|Washington|null           |
+----------+---------------+
		*/

        //方法二：使用toDF
        import spark.implicits._
        val df1 = Seq(
            (1, "a"),
            (1, "a"),
            (2, "a"),
            (1, "b"),
            (2, "b"),
            (3, "b")
        ).toDF("id", "category")
        df1.show
        df1.printSchema()
        /*
+---+--------+
| id|category|
+---+--------+
|  1|       a|
|  1|       a|
|  2|       a|
|  1|       b|
|  2|       b|
|  3|       b|
+---+--------+

root
 |-- id: integer (nullable = false)
 |-- category: string (nullable = true)

        * */
	}
}
