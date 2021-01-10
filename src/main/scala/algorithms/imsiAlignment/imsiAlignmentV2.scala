package algorithms.imsiAlignment
import java.util.Properties
import BatchTrajectorySync._

import com.google.gson.GsonBuilder
import org.apache.http.client.methods.HttpPut
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SparkSession}
import org.apache.spark.sql.functions.col

/** 
* @author zhangjiaqi
* @Description GQ真实数据预处理 
* @param 
* @return  
**/
import java.sql.Timestamp

/**
 * @project zzjz-spk2.1.3hw
 * @autor 杨书轩 on 2020/11/19
 */

object imsiAlignmentV2{

	def main(args:Array[String]):Unit= {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()

		import spark.implicits._

		val sc: SparkContext = spark.sparkContext
		sc.setLogLevel("ERROR")

		val properties = new Properties()
		properties.put("user", "oozie")
		properties.put("password", "oozie")
		val url = "jdbc:mysql://192.168.11.26:3306/test_zjq"

		lazy val sqlc: SQLContext = spark.sqlContext
		var df: DataFrame = sqlc.read.jdbc(url, "gps_points2", properties)

		val imsiCol = "origimsi"
		val latCol = "lat"
		val longCol = "long"
		val timeCol = "occurtime"

		df = df.selectExpr(
			imsiCol,
			"cast(lat as float) lat",
			"cast(long as float) long",
			timeCol
		)
		df.printSchema()
		//筛选出imsi号和临时号对应的记录，分成两个DataFrame
		df = df.na.drop.distinct
				.withColumn("isImsi", col(imsiCol).rlike("^[0-9]*$"))
		val imsiDf = df.filter(col("isImsi"))
		val tempImsiDf = df.filter(!col("isImsi"))

		val tempImsiList = tempImsiDf
				.orderBy(timeCol)
				.collect

		println(tempImsiList(0)(3))
		println(tempImsiList(0)(3).getClass)

		val batchTrajectorySync = BatchTrajectorySync(spark)

		var preDate:Timestamp = new Timestamp(0)
		for (i <- 0 until tempImsiList.length) {
			val curDate:Timestamp = tempImsiList(i)(3).asInstanceOf[Timestamp]
			println(preDate, curDate)

			//批量更新当前时间之前的正常imsi数据
			val imsiDs = imsiDf
				.filter(col(timeCol) > preDate && col(timeCol) <= curDate)
				.select(
				col(imsiCol) as "object_id",
				col(timeCol) as "lastmodified_time",
				col(longCol) as "lng",
				col(latCol) as "lat"
			).as[LastappearedModel]
//			batchTrajectorySync.syncTrajectory(imsiDs, "http://127.0.0.1:5000/batch/update/lastappeared/test")

			//处理当前的临时号数据
			val tempImsi = LastappearedModel(tempImsiList(i)(0).toString,
				tempImsiList(i)(3).asInstanceOf[Timestamp],
				tempImsiList(i)(2).asInstanceOf[Float],
				tempImsiList(i)(1).asInstanceOf[Float]
				)
			println(tempImsi)
			syncTempImsiPoint(tempImsi)

			println("---")
			preDate = curDate
		}

		def syncTempImsiPoint(tempPoint:LastappearedModel, url:String = "http://127.0.0.1:5000/batch/update/lastappeared/test"): Unit ={
			val client = HttpClients.createDefault()
			val gsonBuildr = new GsonBuilder();
			gsonBuildr.registerTypeAdapter(classOf[LastappearedModel], new DifferentNameSerializer());
			val httpPut = new HttpPut(s"${url}/${tempPoint.object_id}")
			val json = gsonBuildr.create().toJson(tempPoint)
			val entity = new StringEntity(json)
			httpPut.setEntity(entity)
			httpPut.setHeader("Accept", "application/json")
			httpPut.setHeader("Content-type", "application/json")
			val response = client.execute(httpPut);
			println(response.getStatusLine().getStatusCode())
			client.close();
		}

		//		//找出关联列nidCol)>44 && df(tcbcnidCol)<=68,
		//		val relationDf = df.filter(df(imsiCol).isNotNull && df(imeiCol).isNotNull)
		//        		.dropDuplicates("origimsi", "imei")
		//		relationDf.show()
		//
		//		//生成imsi到imei的map
		//		val s2eMap = relationDf.rdd
		//				.map(row => (row.getAs[String](imsiCol) -> row.getAs[String](imeiCol)))
		//				.collectAsMap()
		//
		//		//生成imei到imsi的map
		//		val e2sMap = relationDf.rdd
		//				.map(row => (row.getAs[String](imeiCol) -> row.getAs[String](imsiCol)))
		//				.collectAsMap()
		//		val wholeMap = e2sMap ++ s2eMap
		//		println(wholeMap)
		//
		//		//广播关系Map
		//		val broadcastMap = sc.broadcast(wholeMap)
		//
		//		val fillValue = udf((x: String) =>
		//			if(broadcastMap.value.contains(x)){
		//				broadcastMap.value(x)
		//			}
		//			else{
		//				""
		//			}
		//		)
		//		//		df.withColumn("imei", when(df("imei").isNull, "1").otherwise("0"))
		//		df = df.withColumn("new_imsi", when(df(imsiCol).isNull, fillValue(df(imeiCol))).otherwise(df(imsiCol)))
		//				.withColumn("new_imei", when(df(imeiCol).isNull, fillValue(df(imsiCol))).otherwise(df(imeiCol)))
		////		df = df.withColumn("imei", when(df("imei").isNull, fillValue(df("origimsi"))).otherwise(df("imei")))
		////		df = df.withColumn("origimsi", when(df("origimsi").isNull, fillValue(df("imei"))).otherwise(df("origimsi")))
		//		df.show(df.count.toInt, false)
		//
		//		val df2 :DataFrame = df.groupBy("new_imsi").agg(collect_set(tcbcnidCol))
		//				.as("tcbcnid_agg")
		//		df.show(df.count.toInt, false)
	}
}
