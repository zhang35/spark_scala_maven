package algorithms.FlightTrackSegmentation

import java.util.Properties

import com.google.gson.JsonParser
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import java.lang.Math._
import java.text.SimpleDateFormat
import java.sql.Timestamp
import scala.collection.mutable.ListBuffer
import org.apache.commons.math3.geometry.euclidean.twod.Vector2D
/**
 * @ObjectName: FlightTrackSegmentationMain
 * @Description: 提供轨迹分段服务，可选择转角、加速度等分段依据
 * @Auther: zhangjiaqi
 * @Date: 2020/12/04
 * @version : V1.0
 **/

case class PointWithAttributes(planeID:String="", segmentID:Int=0, x:Double=.0, y:Double=.0, turnDegree:Double=.0, timeStamp: Timestamp=new Timestamp(0), time:String="") {
	def setSegmentID(segmentID: Int): PointWithAttributes = this.copy(segmentID=segmentID)
	def setTurnDegree(turnDegree: Double): PointWithAttributes = this.copy(turnDegree=turnDegree)
	def setX(x: Double): PointWithAttributes = this.copy(x=x)
	def setY(y: Double): PointWithAttributes = this.copy(y=y)
	def setPlaneID(planeID: String): PointWithAttributes = this.copy(planeID=planeID)
	def setTimeStamp(timeStamp: Timestamp): PointWithAttributes = this.copy(timeStamp = timeStamp)
	def samePosition(point: PointWithAttributes): Boolean =  point.x == this.x && point.y == this.y
}

object FlightTrackSegmentationMainV2 {
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
		val inputDf = sqlc.read.jdbc(url, "curveTrack", properties)
		inputDf.show
		//        val jsonparam = "<#zzjzParam#>"
//		val jsonparam = """{"inputTableName":"读关系型数据库_1_dt6sfawj","id":[{"name":"id","index":3.0,"datatype":"integer"}],"longitude":[{"name":"long","index":1.0,"datatype":"double"}],"latitude":[{"name":"lat","index":0.0,"datatype":"double"}],"time":[{"name":"occurtime","index":2.0,"datatype":"string"}],"accSpeedThres":"50","turnDegreeThres":"90","RERUNNING":{"rerun":"false","preNodes":[{"id":"读关系型数据库_1_dt6sfawj","checked":true}],"nodeName":"轨迹分段_基于加速度和转角_1","prevInterpreters":[]}}"""
//		val jsonparam =	"""{"inputTableName":"读关系型数据库_1_dt6sfawj","id":[{"name":"id","index":3.0,"datatype":"integer"}],"longitude":[{"name":"long","index":1.0,"datatype":"double"}],"latitude":[{"name":"lat","index":0.0,"datatype":"double"}],"time":[{"name":"occurtime","index":2.0,"datatype":"string"}],"method":{"value":"0","turnDegreeThres":"90"},"RERUNNING":{"rerun":"false","preNodes":[{"id":"读关系型数据库_1_dt6sfawj","checked":true}],"nodeName":"轨迹分段_基于加速度和转角_1","prevInterpreters":[{"paragraphId":"读关系型数据库_1_dt6sfawj","interpreter":"ZZJZ-Algorithm"}]}}"""
//		val jsonparam =	"""{"inputTableName":"读关系型数据库_1_dt6sfawj","id":[{"name":"id","index":3.0,"datatype":"integer"}],"longitude":[{"name":"long","index":1.0,"datatype":"double"}],"latitude":[{"name":"lat","index":0.0,"datatype":"double"}],"time":[{"name":"occurtime","index":2.0,"datatype":"string"}],"method":{"value":"1","accSpeedThres":"1"},"RERUNNING":{"rerun":"false","preNodes":[{"id":"读关系型数据库_1_dt6sfawj","checked":true}],"nodeName":"轨迹分段_基于加速度和转角_1","prevInterpreters":[{"paragraphId":"读关系型数据库_1_dt6sfawj","interpreter":"ZZJZ-Algorithm"}]}}"""
		val jsonparam = """{"inputTableName":"读关系型数据库_1_Qv4hjdVe","id":[{"name":"id","index":0.0,"datatype":"string"}],"longitude":[{"name":"long","index":2.0,"datatype":"double"}],"latitude":[{"name":"lat","index":3.0,"datatype":"double"}],"time":[{"name":"occurtime","index":4.0,"datatype":"string"}],"method":{"value":"0","turnDegreeThres":"30"},"RERUNNING":{"rerun":"true","preNodes":[{"id":"读关系型数据库_1_Qv4hjdVe","checked":true}],"nodeName":"轨迹分段_基于加速度和转角_1复件1","prevInterpreters":[{"paragraphId":"读关系型数据库_1_Qv4hjdVe","interpreter":"ZZJZ-Algorithm"}]}}"""
		//获取用户设置参数
		val parser = new JsonParser()
		val jsonObj = parser.parse(jsonparam).getAsJsonObject

		val inputTableName = jsonObj.get("inputTableName").getAsString
		val idCol = jsonObj.getAsJsonArray("id").get(0).getAsJsonObject.get("name").getAsString
		val timeCol = jsonObj.getAsJsonArray("time").get(0).getAsJsonObject.get("name").getAsString
		val lonCol = jsonObj.getAsJsonArray("longitude").get(0).getAsJsonObject.get("name").getAsString
		val latCol = jsonObj.getAsJsonArray("latitude").get(0).getAsJsonObject.get("name").getAsString
//		val turnDegreeThres = math.abs(jsonObj.get("turnDegreeThres").getAsDouble)
//		val span = jsonObj.get("span").getAsInt
		val turnDegreeThres = 30
		val span = 20
		println(inputTableName, idCol, timeCol, lonCol, latCol, turnDegreeThres, span)

		//数据清洗
		val inRdd: RDD[(String, PointWithAttributes)] = inputDf/*.select(idCol, timeCol, lonCol, latCol).na.drop()*/
				.na.drop(Seq(idCol, timeCol, lonCol, latCol))
				.dropDuplicates(idCol, timeCol)
				.filter(s"""  trim($idCol) != "" and trim($timeCol) != "" and trim($lonCol) != "" and trim($latCol) != "" """)
				.rdd
				.map(r => {
					var x: Double = 0
					var y: Double = 0
					val time = getTimestamp(r.getAs(timeCol).toString)
					var id = ""
					try {
						x = r.getAs(lonCol).toString.toDouble
						y = r.getAs(latCol).toString.toDouble
					} catch {
						case _: Exception => throw new Exception("错误：" + s"$lonCol $latCol 列中含有非数值类型")
					}
					var pt: PointWithAttributes = PointWithAttributes()
					if (math.abs(x) <= 180 && math.abs(y) <= 90) {
						id = r.getAs(idCol).toString.trim
						pt = pt.setX(x).setY(y).setPlaneID(id)
					} else {
						id = "_illegal_xy"
					}
					if (time == null ) {
						id = "_illegal_time"
					} else {
						pt = pt.setTimeStamp(time)
					}
					//					println(id, pt)
					(id, pt)
				}).filter(f => f._1 != "_illegal_xy").filter(f => f._1 != "_illegal_time")
		//将相同飞机的轨迹点,聚合在一起
		val aggredRDD: RDD[(String, ListBuffer[PointWithAttributes])] = inRdd.aggregateByKey(new ListBuffer[PointWithAttributes])(
			(aggregator, value) => aggregator += value,
			(aggregator1, aggregator2) => aggregator1 ++= aggregator2
		)
		val filtedRDD = aggredRDD.mapValues(f => {
			val segmentedListPt: ListBuffer[PointWithAttributes] = SegmentPoints(f, turnDegreeThres, span)
			segmentedListPt.toIterator
		}).flatMap {
			e =>
				for (i <- e._2 ) yield Row(e._1, i.segmentID, i.timeStamp.toString, i.x, i.y, i.turnDegree)
		}
		val structType = StructType(Seq(
			StructField("imsi", StringType),
			StructField("segmentID", IntegerType),
			StructField("time", StringType),
			StructField("longitude", DoubleType),
			StructField("latitude", DoubleType),
			StructField("turnDegree", DoubleType)
		))

		val outDf: DataFrame = sqlc.createDataFrame(filtedRDD, structType)
		outDf
		outDf
//		outDf.createOrReplaceTempView("filterResult")
//		spark.table("filterResult")
//				.write
//				.mode(SaveMode.Overwrite)
//				.jdbc(url, "filterResult", properties)
		outDf.show(outDf.count.toInt, false)
	}

	def SegmentPoints(ListPoint: ListBuffer[PointWithAttributes], turnDegreeThres: Double = .0, span: Int = 1): ListBuffer[PointWithAttributes] = {
		// 按时间排序
		val initialPtList: ListBuffer[PointWithAttributes] = ListPoint.distinct.sortBy(f => f.timeStamp.getTime)
		var retPtList = new ListBuffer[PointWithAttributes]
		val n = initialPtList.length
		if (n <= span || turnDegreeThres > 180 || turnDegreeThres < 0){
			initialPtList
		}
		else{
			//轨迹分段标号
			var id = 1
			//加入起点
			retPtList :+= initialPtList(0).setSegmentID(id)

			var i = 0
			var j = span - 1
			while (j < n) {
				val p1 = initialPtList(i)
				val p2 = initialPtList(i+1)
				val p3 = initialPtList(j)
				//val degree = PointUtil.angle(p1, p2, p3) / math.Pi * 360.0
				// 计算转角
				val degree = getDegree(p1, p2, p3)

				//发现新分段，更新分段号
				if (Math.abs(degree) >= turnDegreeThres) {
					println("seperate! degree:", degree)
					//上一段的终点
					retPtList :+= initialPtList(i+1).setSegmentID(id)
					id += 1
					while (i < j){
						retPtList :+= initialPtList(i+1).setSegmentID(id)
						i += 1
					}
					//下一段的起点
					id += 1
					retPtList :+= initialPtList(i).setSegmentID(id)
					j = i + span - 1
				}
				else{
					retPtList :+= p2.setTurnDegree(degree).setSegmentID(id)
					i += 1
					j += 1
				}
			}
			//加入剩余点
			while (i < n){
				retPtList :+= initialPtList(i).setSegmentID(id)
				i += 1
			}
			retPtList
		}
	}

	def getDegree(p1: PointWithAttributes, p2: PointWithAttributes, p3: PointWithAttributes): Double = {
		var degree: Double = 0.0
		if (!p2.samePosition(p1) && !p3.samePosition(p2)) {
			//向量p1p2
			val vector1 = new Vector2D(p2.x - p1.x, p2.y - p1.y)
			//向量p2p3
			val vector2 = new Vector2D(p3.x - p2.x, p3.y - p2.y)

			val angle = Vector2D.angle(vector1, vector2)

			degree = angle / PI * 180.0
			if (crossProduct(p1, p2, p3) < 0)
				degree = -degree
		}
		degree
	}

	def crossProduct(p1: PointWithAttributes, p2: PointWithAttributes, p3: PointWithAttributes): Double = {
		val crossProduct = (p2.x - p1.x) * (p3.y - p2.y) - (p2.y - p1.y) * (p3.x - p2.x)
		crossProduct
	}

	def getTimestamp(str: String): Timestamp = {
		val regex1 = "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{1,3}" //2018-07-27 18:31:18.000
		val regex2 = "[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{1,3}" //2018/07/27 18:31:18.000
		val regex3 = "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}" //2018-07-27 18:31:18
		val regex4 = "[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}" //2018/07/27 18:31:18
		try {
			if (str.matches(regex1)) {
				val dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
				val lt = dataFormat.parse(str).getTime
				new java.sql.Timestamp(lt)
			} else if (str.matches(regex2)) {
				val dataFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS")
				val lt = dataFormat.parse(str).getTime
				new java.sql.Timestamp(lt)
			} else if (str.matches(regex3)) {
				val dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
				val lt = dataFormat.parse(str).getTime
				new java.sql.Timestamp(lt)
			} else if (str.matches(regex4)) {
				val dataFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
				val lt = dataFormat.parse(str).getTime
				new java.sql.Timestamp(lt)
			}
			else {
				null
			}
		}
		catch {
			case _: Exception => null
		}
	}
}
