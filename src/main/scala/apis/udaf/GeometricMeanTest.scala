package apis.udaf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

/**
 * @ClassName: useUDAF
 * @Description: TODO
 * @Auther: zhangjiaqi
 * @Date: 2020/11/24 11:56
 * @version : V1.0
 **/
object GeometricMeanTest {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()

		//让世界清静些
		spark.sparkContext.setLogLevel("WARN")

		//Register the UDAF with Spark SQL
		spark.udf.register("gm", new GeometricMean)
		val ids = spark.range(1, 20)
		ids.createOrReplaceTempView("ids")
		val df = spark.sql("select id, id % 3 as group_id from ids")
		df.createOrReplaceTempView("simple")
		df.show
/*
+---+--------+
| id|group_id|
+---+--------+
|  1|       1|
|  2|       2|
|  3|       0|
|  4|       1|
|  5|       2|
|  6|       0|
|  7|       1|
|  8|       2|
|  9|       0|
| 10|       1|
| 11|       2|
| 12|       0|
| 13|       1|
| 14|       2|
| 15|       0|
| 16|       1|
| 17|       2|
| 18|       0|
| 19|       1|
+---+--------+
**/


		// Create an instance of UDAF GeometricMean.
		val gm = new GeometricMean

		//
		// Show the geometric mean of values of column "id".
		df.groupBy("group_id").agg(gm(col("id")).as("GeometricMean")).show()
		/*
+--------+-----------------+
|group_id|    GeometricMean|
+--------+-----------------+
|       0|8.981385496571725|
|       1|7.301716979342118|
|       2|7.706253151292568|
+--------+-----------------+
		**/

		// Invoke the UDAF by its assigned name.
		df.groupBy("group_id").agg(expr("gm(id) as GeometricMean")).show()
		/*
+--------+-----------------+
|group_id|    GeometricMean|
+--------+-----------------+
|       0|8.981385496571725|
|       1|7.301716979342118|
|       2|7.706253151292568|
+--------+-----------------+
		**/
	}
}
