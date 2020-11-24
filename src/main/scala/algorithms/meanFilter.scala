package algorithms
import org.apache.spark.sql.SparkSession
import apis.udaf.{Employee, GeometricMean, Median, TypedMyAverageAggregator}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col
/**
  * @author zhang35 
  * @date 2020/11/22 8:28 PM
  */


object meanFilter {

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .master("local[*]")
            .appName("SparkExample")
            .getOrCreate()

        import spark.implicits._
        val df = Seq(
            ("Michael", 3000),
            ("Justin", 3500),
            ("Andy", 4500),
            ("Berta", 4000)
        ).toDF("name", "salary")

        df.createOrReplaceTempView("simple")

//        val ds = df.as[Employee]
//        val averageSalary = TypedMyAverageAggregator.toColumn.name("average_salary")
//        val result = ds.select(averageSalary)
//        result.show()
//        df.groupBy("group_id").agg(expr("gm(id) as GeometricMean")).show()
        val windowSpec = Window.rowsBetween(-1, 1)
        // Create an instance of UDAF GeometricMean.
        val gm = new GeometricMean
        val median = new Median
        val r2 = df.withColumn("geo_mean", median(col("salary")).over(windowSpec))
        r2.show
//        r2.show
    }
}
/*
I found window functions and UDAFs in the Spark API to be a good way implementing the median filtering.

Registering a UDAF:

sqlContext.udf().register("medianFilter", medianFilterUDAF);

Calling a UDAF over a window:

Column col1 = org.apache.spark.sql.functions.callUDF("medianFilter", intDF.col("column1")).over(Window.orderBy("columnA", "columnB").rowsBetween(-windowSize, windowSize));

medianFilterUDAF is an object of a custom UDAF that performs median filtering of elements over a window. You have to override the methods of the UDAF class viz. dataType(), evaluate(Row buffer), initialize(MutableAggregationBuffer buffer), update(MutableAggregationBuffer buffer, Row row), merge(MutableAggregationBuffer buffer1, Row buffer2), inputSchema(), bufferSchema(), and deterministic().

So for median filtering, you have to sort the elements of the buffer and return the middle element in the evaluate method.
 */

/*
0

Well I didn't understand the vector part, however this is my approach (I bet there are better ones):

val a = sc.parallelize(Seq(1, 2, -1, 12, 3, 0, 3))
val n = a.count() / 2

println(n) // outputs 3

val b = a.sortBy(x => x).zipWithIndex()
val median = b.filter(x => x._2 == n).collect()(0)._1  // this part doesn't look nice, I hope someone tells me how to improve it, maybe zero?

println(median) // outputs 2
b.collect().foreach(println) // (-1,0) (0,1) (1,2) (2,3) (3,4) (3,5) (12,6)

The trick is to sort your dataset using sortBy, then zip the entries with their index using zipWithIndex and then get the middle entry, note that I set an odd number of samples, for simplicity but the essence is there, besides you have to do this with every column of your dataset.
 */