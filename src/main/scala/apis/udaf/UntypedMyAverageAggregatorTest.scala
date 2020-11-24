package apis.udaf

import org.apache.spark.sql.{SparkSession, functions}

/**
  * @author zhang35
  * @date 2020/11/22 8:28 PM
  */

object UntypedMyAverageAggregatorTest {

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .master("local[*]")
            .appName("SparkExample")
            .getOrCreate()

        // Register the function to access it

        // spark 3.0版本支持，2.3不支持
//        spark.udf.register("myAverage", functions.udaf(UntypedMyAverageAggregator))

        import spark.implicits._
        val df = Seq(
            ("Michael", 3000),
            ("Justin", 3500),
            ("Andy", 4500),
            ("Berta", 4000)
        ).toDF("name", "salary")
//
        df.createOrReplaceTempView("employees")
	    df.show()
        val result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
        result.show()
        /*
在spark 3.0版本下，注册后可正常运行：
spark.udf.register("myAverage", functions.udaf(UntypedMyAverageAggregator))
+--------------+
|average_salary|
+--------------+
|        3750.0|
+--------------+
         */
    }
}
