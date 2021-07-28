import org.apache.spark.SparkException
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @ClassName: test
 * @Description: TODO
 * @Auther: zhangjiaqi
 * @Date: 2021/2/1 9:42
 * @version : V1.0
 **/
object test {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()

		import spark.implicits._
		val df1 = Seq(
			(1, "a"),
			(1, "a"),
			(2, "a"),
			(1, "b"),
			(2, "b"),
			(3, "b")
		).toDF("id", "category")
		val colName = "category"
		val df3 = df1.filter(s"CAST(`${colName}` as `double`) IS NULL")
		if (df3.count() > 0){
			throw new Exception(s"Given column `${colName}` has non-numerical value")
		}
		df3.printSchema()
		df3.show()
//		if (df1.schema(colName).dataType.typeName != "integer")
//			println(" name is 'string' column")
//		val sqlExpr = s"""cast(`${colName}` as `double`) as `${colName}`"""
//		val df2 = df1.selectExpr(sqlExpr)
//		df2.printSchema()
//		df2.show()
	}
}
