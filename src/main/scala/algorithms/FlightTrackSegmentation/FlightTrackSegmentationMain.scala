package algorithms.FlightTrackSegmentation

import java.util.Properties

import algorithms.FlightTrackMedianFilter.util.MedianFilter
import algorithms.FlightTrackSegmentation.util.FlightTrackSegmentation
import com.google.gson.{Gson, JsonParser}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

/**
 * @ObjectName: FlightTrackSegmentationMain
 * @Description: 提供轨迹分段服务，可选择转角、加速度等分段依据
 * @Auther: zhangjiaqi
 * @Date: 2020/12/04
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
		val inputDf = sqlc.read.jdbc(url, "gps_points", properties)
//		inputDF.show

		//        val jsonparam = "<#zzjzParam#>"
//		val jsonparam = """{"inputTableName":"读关系型数据库_1_dt6sfawj","id":[{"name":"id","index":3.0,"datatype":"integer"}],"longitude":[{"name":"long","index":1.0,"datatype":"double"}],"latitude":[{"name":"lat","index":0.0,"datatype":"double"}],"time":[{"name":"occurtime","index":2.0,"datatype":"string"}],"accSpeedThres":"50","turnDegreeThres":"90","RERUNNING":{"rerun":"false","preNodes":[{"id":"读关系型数据库_1_dt6sfawj","checked":true}],"nodeName":"轨迹分段_基于加速度和转角_1","prevInterpreters":[]}}"""
		val jsonparam =	"""{"inputTableName":"读关系型数据库_1_dt6sfawj","id":[{"name":"id","index":3.0,"datatype":"integer"}],"longitude":[{"name":"long","index":1.0,"datatype":"double"}],"latitude":[{"name":"lat","index":0.0,"datatype":"double"}],"time":[{"name":"occurtime","index":2.0,"datatype":"string"}],"method":{"value":"0","turnDegreeThres":"90"},"RERUNNING":{"rerun":"false","preNodes":[{"id":"读关系型数据库_1_dt6sfawj","checked":true}],"nodeName":"轨迹分段_基于加速度和转角_1","prevInterpreters":[{"paragraphId":"读关系型数据库_1_dt6sfawj","interpreter":"ZZJZ-Algorithm"}]}}"""
//		val jsonparam =	"""{"inputTableName":"读关系型数据库_1_dt6sfawj","id":[{"name":"id","index":3.0,"datatype":"integer"}],"longitude":[{"name":"long","index":1.0,"datatype":"double"}],"latitude":[{"name":"lat","index":0.0,"datatype":"double"}],"time":[{"name":"occurtime","index":2.0,"datatype":"string"}],"method":{"value":"1","accSpeedThres":"1"},"RERUNNING":{"rerun":"false","preNodes":[{"id":"读关系型数据库_1_dt6sfawj","checked":true}],"nodeName":"轨迹分段_基于加速度和转角_1","prevInterpreters":[{"paragraphId":"读关系型数据库_1_dt6sfawj","interpreter":"ZZJZ-Algorithm"}]}}"""

		//获取用户设置参数
		val parser = new JsonParser()
		val jsonObj = parser.parse(jsonparam).getAsJsonObject

		val inputTableName = jsonObj.get("inputTableName").getAsString
		val idCol = jsonObj.getAsJsonArray("id").get(0).getAsJsonObject.get("name").getAsString
		val timeCol = jsonObj.getAsJsonArray("time").get(0).getAsJsonObject.get("name").getAsString
		val lonCol = jsonObj.getAsJsonArray("longitude").get(0).getAsJsonObject.get("name").getAsString
		val latCol = jsonObj.getAsJsonArray("latitude").get(0).getAsJsonObject.get("name").getAsString
		val method = jsonObj.getAsJsonObject("method").get("value").getAsInt
		println(inputTableName, idCol, timeCol, lonCol, latCol, method)
		// 分段依据，0：按转角；1：按加速度
		val outDf:DataFrame = if (method == 0){
			val turnDegreeThres = math.abs(jsonObj.getAsJsonObject("method").get("turnDegreeThres").getAsDouble)
			new FlightTrackSegmentation(spark.sqlContext).run(inputDf=inputDf, idCol=idCol, timeCol=timeCol, lonCol=lonCol, latCol=latCol, method=method, turnDegreeThres = turnDegreeThres)
		}
		else if (method == 1){
			val accSpeedThres = math.abs(jsonObj.getAsJsonObject("method").get("accSpeedThres").getAsDouble)
			new FlightTrackSegmentation(spark.sqlContext).run(inputDf=inputDf, idCol=idCol, timeCol=timeCol, lonCol=lonCol, latCol=latCol, method=method, accSpeedThres = accSpeedThres)
		}
		else{
			null
		}
		outDf.createOrReplaceTempView("filterResult")
		spark.table("filterResult")
				.write
				.mode(SaveMode.Overwrite)
				.jdbc(url, "filterResult", properties)
		outDf.show
	}
}
