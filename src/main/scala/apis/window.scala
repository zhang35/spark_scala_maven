    package apis
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._

    object window {
        def main(args: Array[String]): Unit = {
            val spark: SparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("SparkExample")
                .getOrCreate()

            import spark.implicits._
            val simpleData = Seq(("James", "Sales", 3000),
                ("Michael", "Sales", 4600),
                ("Robert", "Sales", 4100),
                ("Maria", "Finance", 3000),
                ("James", "Sales", 3000),
                ("Scott", "Finance", 3300),
                ("Jen", "Finance", 3900),
                ("Jeff", "Marketing", 3000),
                ("Kumar", "Marketing", 2000),
                ("Saif", "Sales", 4100)
            )
            val df = simpleData.toDF("employee_name", "department", "salary")
            df.show()
            /*

+-------------+----------+------+
|employee_name|department|salary|
+-------------+----------+------+
|        James|     Sales|  3000|
|      Michael|     Sales|  4600|
|       Robert|     Sales|  4100|
|        Maria|   Finance|  3000|
|        James|     Sales|  3000|
|        Scott|   Finance|  3300|
|          Jen|   Finance|  3900|
|         Jeff| Marketing|  3000|
|        Kumar| Marketing|  2000|
|         Saif|     Sales|  4100|
+-------------+----------+------+
            * */
            //row_number() window function is used to give the sequential row number starting from 1 to the result of each window partition.
            val windowSpec = Window.partitionBy("department").orderBy("salary")
            df.withColumn("row_number", row_number.over(windowSpec))
                .show
            /*
|employee_name|department|salary|row_number|
+-------------+----------+------+----------+
|        James|     Sales|  3000|         1|
|        James|     Sales|  3000|         2|
|       Robert|     Sales|  4100|         3|
|         Saif|     Sales|  4100|         4|
|      Michael|     Sales|  4600|         5|
|        Maria|   Finance|  3000|         1|
|        Scott|   Finance|  3300|         2|
|          Jen|   Finance|  3900|         3|
|        Kumar| Marketing|  2000|         1|
|         Jeff| Marketing|  3000|         2|
+-------------+----------+------+----------+
            */

            //rank() window function is used to provide a rank to the result within a window partition. This function leaves gaps in rank when there are ties.
            df.withColumn("rank",rank().over(windowSpec))
                .show()
/*

+-------------+----------+------+----+
|employee_name|department|salary|rank|
+-------------+----------+------+----+
|        James|     Sales|  3000|   1|
|        James|     Sales|  3000|   1|
|       Robert|     Sales|  4100|   3|
|         Saif|     Sales|  4100|   3|
|      Michael|     Sales|  4600|   5|
|        Maria|   Finance|  3000|   1|
|        Scott|   Finance|  3300|   2|
|          Jen|   Finance|  3900|   3|
|        Kumar| Marketing|  2000|   1|
|         Jeff| Marketing|  3000|   2|
+-------------+----------+------+----+
*/
            val windowSpecAgg  = Window.partitionBy("department")
            val aggDF = df.withColumn("row",row_number.over(windowSpec))
                .withColumn("avg", avg(col("salary")).over(windowSpecAgg))
                .withColumn("sum", sum(col("salary")).over(windowSpecAgg))
                .withColumn("min", min(col("salary")).over(windowSpecAgg))
                .withColumn("max", max(col("salary")).over(windowSpecAgg))
                //.where(col("row")===1).select("department","avg","sum","min","max")
                .show()
        }
/*
+-------------+----------+------+---+------+-----+----+----+
|employee_name|department|salary|row|   avg|  sum| min| max|
+-------------+----------+------+---+------+-----+----+----+
|        James|     Sales|  3000|  1|3760.0|18800|3000|4600|
|        James|     Sales|  3000|  2|3760.0|18800|3000|4600|
|       Robert|     Sales|  4100|  3|3760.0|18800|3000|4600|
|         Saif|     Sales|  4100|  4|3760.0|18800|3000|4600|
|      Michael|     Sales|  4600|  5|3760.0|18800|3000|4600|
|        Maria|   Finance|  3000|  1|3400.0|10200|3000|3900|
|        Scott|   Finance|  3300|  2|3400.0|10200|3000|3900|
|          Jen|   Finance|  3900|  3|3400.0|10200|3000|3900|
|        Kumar| Marketing|  2000|  1|2500.0| 5000|2000|3000|
|         Jeff| Marketing|  3000|  2|2500.0| 5000|2000|3000|
+-------------+----------+------+---+------+-----+----+----+

//添加.where(col("row")===1).select("department","avg","sum","min","max")
+----------+------+-----+----+----+
|department|   avg|  sum| min| max|
+----------+------+-----+----+----+
|     Sales|3760.0|18800|3000|4600|
|   Finance|3400.0|10200|3000|3900|
| Marketing|2500.0| 5000|2000|3000|
+----------+------+-----+----+----+
 */

    }
