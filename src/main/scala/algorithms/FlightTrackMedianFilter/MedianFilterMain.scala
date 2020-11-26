package algorithms.FlightTrackMedianFilter

import java.util.Properties

import algorithms.FlightTrackMedianFilter.util.MedianFilter
import com.google.gson.{Gson, JsonParser}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

/**
  * @author zhang35
  * @date 2020/11/22 8:28 PM
  */

object MedianFilterMain {

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .master("local[*]")
            .appName("SparkExample")
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
	    //模拟数据
        val properties = new Properties()
        properties.put("user","oozie")
        properties.put("password","oozie")
        val url = "jdbc:mysql://192.168.11.26:3306/test_zjq"
        lazy val sqlc: SQLContext =spark.sqlContext
        val inputDF = sqlc.read.jdbc(url,"gps_points",properties)
        inputDF.show

//        val jsonparam = "<#zzjzParam#>"
        val jsonparam = """{"inputTableName":"读关系型数据库_1_iwOwjBzb","id":[{"name":"id","index":3.0,"datatype":"integer"}],"longitude":[{"name":"long","index":1.0,"datatype":"double"}],"latitude":[{"name":"lat","index":0.0,"datatype":"double"}],"time":[{"name":"occurtime","index":2.0,"datatype":"string"}],"windowSize":"3","thresh":"0.8","RERUNNING":{"rerun":"false","preNodes":[{"id":"读关系型数据库_1_iwOwjBzb","checked":true}],"nodeName":"轨迹点中值过滤_1","prevInterpreters":[{"paragraphId":"读关系型数据库_1_iwOwjBzb","interpreter":"ZZJZ-Algorithm"}]}}"""
        val gson = new Gson()
        val pGson: java.util.Map[String, String] = gson.fromJson(jsonparam, classOf[java.util.Map[String, String]])
        val parser = new JsonParser()
        val jsonObj = parser.parse(jsonparam).getAsJsonObject

        //获取用户设置参数
        val inputTableName = pGson.get("inputTableName")
        val idCol = jsonObj.getAsJsonArray("id").get(0).getAsJsonObject.get("name").getAsString
        val timeCol = jsonObj.getAsJsonArray("time").get(0).getAsJsonObject.get("name").getAsString
        val lonCol = jsonObj.getAsJsonArray("longitude").get(0).getAsJsonObject.get("name").getAsString
        val latCol = jsonObj.getAsJsonArray("latitude").get(0).getAsJsonObject.get("name").getAsString
        val windowSize = math.abs(pGson.get("windowSize").toInt)
        val thres = math.abs(pGson.get("windowSize").toDouble)

        val outDf: DataFrame = new MedianFilter(spark.sqlContext).run(inputDF, idCol, timeCol, lonCol, latCol, windowSize, thres)
	    outDf.show()
        outDf.createOrReplaceTempView("filterResult")
        spark.table("filterResult")
                .write
                .mode(SaveMode.Overwrite)
                .jdbc(url, "filterResult", properties)
//        outputrdd.put("<#zzjzRddName#>", outDf)
//        outDf.createOrReplaceTempView("<#zzjzRddName#>")

//        val windowSpec = Window.rowsBetween(-1, 1)
//        // Create an instance of UDAF GeometricMean.
//        val median = new Median
//        df.withColumn("median", median(col("salary")).over(windowSpec))
//                .show()
        /*
+-------+------+------+
|   name|salary|median|
+-------+------+------+
|Michael|     1|   1.5|
| Justin|     2|   2.0|
|   Andy|     4|   4.0|
|  Berta|     5|   5.0|
|Machael|     8|   8.0|
| Jastin|     9|   9.0|
|   Aady|    10|   9.0|
|  Barta|     3|   3.0|
|Mbchael|     1|   2.0|
| Jbstin|     2|   2.0|
|   Abdy|     3|   3.0|
|  Bbrta|     4|   3.5|
+-------+------+------+
        **/
    }
}
