package algorithms.FlightTrackSegmentation

import java.util.Properties

import algorithms.FlightTrackMedianFilter.util.MedianFilter
import algorithms.FlightTrackSegmentation.util.FlightTrackSegmentation
import com.google.gson.{Gson, JsonParser}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

/**
 * @ClassName: FlightTrackSegmentationMain
 * @Description: TODO
 * @Auther: zhangjiaqi
 * @Date: 2020/11/27 15:52
 * @version : V1.0
 **/
object FlightTrackSegmentationMain {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		spark.sparkContext.setLogLevel("WARN")
		//模拟数据
		val properties = new Properties()
		properties.put("user", "oozie")
		properties.put("password", "oozie")
		val url = "jdbc:mysql://192.168.11.26:3306/test_zjq"
		lazy val sqlc: SQLContext = spark.sqlContext
		val inputDF = sqlc.read.jdbc(url, "gps_points", properties)
//		inputDF.show

		//        val jsonparam = "<#zzjzParam#>"
		val jsonparam = """{"inputTableName":"读关系型数据库_1_dt6sfawj","id":[{"name":"id","index":3.0,"datatype":"integer"}],"longitude":[{"name":"long","index":1.0,"datatype":"double"}],"latitude":[{"name":"lat","index":0.0,"datatype":"double"}],"time":[{"name":"occurtime","index":2.0,"datatype":"string"}],"accSpeedThres":"50","turnDegreeThres":"90","RERUNNING":{"rerun":"false","preNodes":[{"id":"读关系型数据库_1_dt6sfawj","checked":true}],"nodeName":"轨迹分段_基于加速度和转角_1","prevInterpreters":[]}}"""
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
		val turnDegreeThres = math.abs(pGson.get("turnDegreeThres").toDouble)
		val accSpeedThres = math.abs(pGson.get("accSpeedThres").toDouble)
		println(inputTableName, idCol, timeCol, lonCol, latCol, turnDegreeThres, accSpeedThres)
		val outDf: DataFrame = new FlightTrackSegmentation(spark.sqlContext).run(inputDF, idCol, timeCol, lonCol, latCol, turnDegreeThres, accSpeedThres)
		outDf.show()
//		outDf.show()
//		outDf.createOrReplaceTempView("filterResult")
//		spark.table("filterResult")
//				.write
//				.mode(SaveMode.Overwrite)
//				.jdbc(url, "filterResult", properties)
	}
}
