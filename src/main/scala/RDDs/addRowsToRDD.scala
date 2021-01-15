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
object addRowsToRDD {

	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext

		import spark.implicits._
		val  df = sc.parallelize(Seq(
			(1.0, 2.0), (0.0, -1.0),
			(3.0, 4.0), (6.0, -2.3))).toDF("x", "y")

		def transformRow(row: Row): Row =  Row.fromSeq(row.toSeq ++ Array[Any](-1, 1))

		def transformRows(iter: Iterator[Row]): Iterator[Row] = iter.map(transformRow)

		val newSchema = StructType(df.schema.fields ++ Array(
			StructField("z", IntegerType, false), StructField("v", IntegerType, false)))

		spark.createDataFrame(df.rdd.mapPartitions(transformRows), newSchema).show
		/*
		+---+----+---+---+
		|  x|   y|  z|  v|
		+---+----+---+---+
		|1.0| 2.0| -1|  1|
		|0.0|-1.0| -1|  1|
		|3.0| 4.0| -1|  1|
		|6.0|-2.3| -1|  1|
		+---+----+---+---+
		**/
	}
}
