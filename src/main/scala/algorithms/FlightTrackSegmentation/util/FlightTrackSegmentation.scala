package algorithms.FlightTrackSegmentation.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.mutable.ListBuffer

class FlightTrackSegmentation(sqlc: SQLContext) {

	def run(inputDf: DataFrame, idCol: String, timeCol: String, lonCol: String, latCol: String, turnDegreeThres: Double, accSpeedThres: Double): DataFrame = {
		val inputRDD: RDD[(String, PointWithSegID)] = inputDf/*.select(idCol, timeCol, lonCol, latCol).na.drop()*/
				.na.drop(Seq(idCol, timeCol, lonCol, latCol))
				.dropDuplicates(idCol, timeCol)
				.filter(s"""  trim($idCol) != "" and trim($timeCol) != "" and trim($lonCol) != "" and trim($latCol) != "" """)
				.rdd
				.map(r => {
					var x: Double = 0
					var y: Double = 0
					val time = PointUtil.getTimestamp(r.getAs(timeCol).toString)
					var id = ""
					var pt: PointWithSegID = new PointWithSegID(0, 0)
					try {
						x = r.getAs(lonCol).toString.toDouble
						y = r.getAs(latCol).toString.toDouble
					} catch {
						case _: Exception => throw new Exception("错误：" + s"$lonCol $latCol 列中含有非数值类型")
					}
					if (math.abs(x) <= 180 && math.abs(y) <= 90) {
						pt = new PointWithSegID(x, y)
						id = r.getAs(idCol).toString.trim
						pt.setPlaneID(id)
					} else {
						id = "_illegal_xy"
					}
					if (time == null ) {
						id = "_illegal_time"
					} else {
						pt.timeStamp = time
					}
					(id, pt)
				}).filter(f => f._1 != "_illegal_xy").filter(f => f._1 != "_illegal_time")

		//将相同飞机的轨迹点,聚合在一起
		val aggredRDD: RDD[(String, ListBuffer[PointWithSegID])] = inputRDD.aggregateByKey(new ListBuffer[PointWithSegID])(
			(aggregator, value) => aggregator += value,
			(aggregator1, aggregator2) => aggregator1 ++= aggregator2
		)
		val filtedRDD = aggredRDD.mapValues(f => {
			val segmentUtil = new FlightTrackSegmentationUtil()
			val segmentedListPt: ListBuffer[PointWithSegID] = segmentUtil.SegmentPoints(f, turnDegreeThres, accSpeedThres)
			segmentedListPt.toIterator
		}).flatMap {
			e =>
				for (i <- e._2 ) yield Row(e._1, i.timeStamp.toString, i.x, i.y)
		}
		val structType = StructType(Seq(
			StructField("imsi", StringType),
			StructField("time", StringType),
			StructField("Longitude", DoubleType),
			StructField("Latitude", DoubleType)
		))

		val outDf: DataFrame = sqlc.createDataFrame(filtedRDD, structType)
		outDf
	}
}
