package algorithms

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
/**
  * @author zhang35
  * @date 2020/11/22 8:28 PM
  */
case class Average(var sum: Long, var count: Long)

object MyAverage extends Aggregator[Long, Average, Double] {
    // A zero value for this aggregation. Should satisfy the property that any b + zero = b
    def zero: Average = Average(0L, 0L)
    // Combine two values to produce a new value. For performance, the function may modify `buffer`
    // and return it instead of constructing a new object
    def reduce(buffer: Average, data: Long): Average = {
        buffer.sum += data
        buffer.count += 1
        buffer
    }
    // Merge two intermediate values
    def merge(b1: Average, b2: Average): Average = {
        b1.sum += b2.sum
        b1.count += b2.count
        b1
    }
    // Transform the output of the reduction
    def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count
    // Specifies the Encoder for the intermediate value type
    def bufferEncoder: Encoder[Average] = Encoders.product
    // Specifies the Encoder for the final output value type
    def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

object UDAF {

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .master("local[*]")
            .appName("SparkExample")
            .getOrCreate()

        // Register the function to access it
        spark.udf.register("myAverage", functions.udaf(MyAverage))

        import spark.implicits._
        val df = Seq(
            ("Michael", 3000),
            ("Justin", 3500),
            ("Andy", 4500),
            ("Berta", 4000)
        ).toDF("name", "salary")

        df.createOrReplaceTempView("employees")
        val result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
        result.show()
        /*
+--------------+
|average_salary|
+--------------+
|        3750.0|
+--------------+
         */
    }
}
