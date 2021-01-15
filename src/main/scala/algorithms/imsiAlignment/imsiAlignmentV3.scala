package algorithms.imsiAlignment

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import java.lang.Math._
import scala.collection.mutable.ListBuffer

/** 
* @author zhangjiaqi
* @Description GQ真实数据预处理 
* @param 
* @return  
**/
import java.sql.Timestamp



object imsiAlignmentV3{
	def convertDegreesToRadians(degree: Double): Double = {
		(degree * PI) / 180
	}

	//返回地球两点距离，单位：m
	def distanceOf2point(long1:Float, lat1:Float, long2:Float, lat2:Float): Double = {
		val EARTH_RADIUS = 6378137
		val radLat1 = convertDegreesToRadians(lat1)
		val radLng1 = convertDegreesToRadians(long1)
		val radLat2 = convertDegreesToRadians(lat2)
		val radLng2 = convertDegreesToRadians(long2)
		val a = radLat1 - radLat2
		val b = radLng1 - radLng2
		val distance = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2)
				+ Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2))) * EARTH_RADIUS
		distance
	}

	//返回t1 t2的秒数差
	def getTimeInterval(t1: Timestamp, t2: Timestamp): Long ={
		abs(t1.getTime - t2.getTime) / 1000
	}

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
		//输入按小时分箱的DataFrame
		var df: DataFrame = sqlc.read.jdbc(url, "gps_points_bucket", properties)

		val imsiCol = "origimsi"
		val latCol = "lat"
		val longCol = "long"
		val timeCol = "occurtime"
		val bucketCol = "bucket_id"
		val imsiExp = "^[0-9]*$"
		val timeUpperBound = 130
		val timeLowerBound = 110
		val distUpperBound = 100000
		val distLowerBound = 1000
		val numPartitons = 24

		df = df.selectExpr(
			imsiCol,
			"cast(lat as float) lat",
			"cast(long as float) long",
			timeCol,
			bucketCol
		)
		df.printSchema()

		//标记是imsi号的列
		df = df.na.drop.distinct
				.withColumn("isImsi", col(imsiCol).rlike(imsiExp))
		//按箱子分区
		val bucketDF = df.repartition(numPartitons, col(bucketCol))
				//每个分区内部按时间排序
				.sortWithinPartitions(col(timeCol))

//		bucketDF.rdd.mapPartitionsWithIndex{
//			//参数较多时用模式匹配
//			case (num, datas) => {
//				datas.map((_, "分区号: " + num))
//			}
//		}.collect().foreach(println)

		def transformRows(iter: Iterator[Row]): Iterator[Row] = {
			val rows: Seq[Row] = iter.toSeq
			var rowsBuffer: ListBuffer[Row] = new ListBuffer[Row]
			//记录临时号->imsi号对应关系
			var imsiMap: Map[String, String] = Map()
			var i = 0
			for (i <- 1 until rows.length){
				//如果是临时号
				val newImsi: String = if (rows(i)(5).asInstanceOf[Boolean]) {
					rows(i)(0).asInstanceOf[String]
				}
				else {
					var j = i - 1
					val timeI: Timestamp = rows(i)(3).asInstanceOf[Timestamp]
					//找出时间相差范围在110s - 130s之间的候选点集
					var tempBuffer: ListBuffer[Row] = new ListBuffer[Row]
					while (j >= 0 && getTimeInterval(timeI, rows(j)(3).asInstanceOf[Timestamp]) < timeLowerBound){
						j -= 1
					}
					while (j >= 0 && getTimeInterval(timeI, rows(j)(3).asInstanceOf[Timestamp]) < timeUpperBound){
						tempBuffer :+= rows(j)
						j -= 1
					}
					val long1 = rows(i)(2).asInstanceOf[Float]
					val lat1 = rows(i)(1).asInstanceOf[Float]

					//初始值设为本身imsi
					val originImsi: String = rows(i)(0).asInstanceOf[String]
					var tempImsi:String = originImsi
					//在候选点集中找出距离在阈值范围内的点, 作为对齐上的目标点
					for (row <- tempBuffer) {
						val long2 = row(2).asInstanceOf[Float]
						val lat2 = row(1).asInstanceOf[Float]
						val dist = distanceOf2point(long1, lat1, long2, lat2)
						if (dist <= distUpperBound && dist >= distLowerBound){
							val targetImsi = row(0).asInstanceOf[String]
							tempImsi = if (imsiMap.contains(targetImsi)){
								imsiMap(targetImsi)
							}
							else{
								targetImsi
							}
						}
					}
					//添加临时号到imsi号的对应关系
					imsiMap += (originImsi -> tempImsi)
					tempImsi
				}
				rowsBuffer :+= Row.fromSeq(rows(i).toSeq ++ Array[String](newImsi))
			}
			println(imsiMap)
			rowsBuffer.toIterator
		}

		val newSchema = StructType(bucketDF.schema.fields ++ Array(
			StructField("newImsi", StringType, true)))

		spark.createDataFrame(bucketDF.rdd.mapPartitions(transformRows), newSchema).show
	}
}
