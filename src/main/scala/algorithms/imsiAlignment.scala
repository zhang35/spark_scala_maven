package algorithms

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{collect_set, udf, when}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**
 * @ClassName: imsiAlignment
 * @Description: TODO
 * @Auther: zhangjiaqi
 * @Date: 2020/11/19 10:44
 * @version : V1.0
 **/
object imsiAlignment{

	def main(args:Array[String]):Unit= {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()

		val sc: SparkContext = spark.sparkContext
		sc.setLogLevel("ERROR")

		val properties = new Properties()
		properties.put("user","oozie")
		properties.put("password","oozie")
		val url = "jdbc:mysql://192.168.11.26:3306/test_zjq"

		lazy val sqlc: SQLContext =spark.sqlContext
		var df: DataFrame = sqlc.read.jdbc(url,"gps_points1",properties)

		val imsiCol = "origimsi"
		val imeiCol = "imei"
		val tcbcnidCol = "tcbcnid"

		//Chaining multiple options
//		var df = spark.read.options(
//			Map("inferSchema"->"true","delimiter"->",","header"->"true"))
//			.csv(filePath)
		df.show(false)
		df.printSchema()

		df = df.withColumn("origimsi",
			when(df(imsiCol).isNull && df(tcbcnidCol)<=30, "000")
					.when(df(imsiCol).isNull && df(tcbcnidCol)>30 && df(tcbcnidCol)<=44, "111")
					.when(df(imsiCol).isNull && df(tcbcnidCol)>44 && df(tcbcnidCol)<=68, "222")
					.when(df(imsiCol).isNull && df(tcbcnidCol)>68 && df(tcbcnidCol)<=89, "333")
					.when(df(imsiCol).isNull && df(tcbcnidCol)>89, "444")
					.otherwise(df(imsiCol)))
		df.show(df.count.toInt, false)
		//找出关联列
		val relationDf = df.filter(df(imsiCol).isNotNull && df(imeiCol).isNotNull)
        		.dropDuplicates("origimsi", "imei")
		relationDf.show()

		//生成imsi到imei的map
		val s2eMap = relationDf.rdd
				.map(row => (row.getAs[String](imsiCol) -> row.getAs[String](imeiCol)))
				.collectAsMap()

		//生成imei到imsi的map
		val e2sMap = relationDf.rdd
				.map(row => (row.getAs[String](imeiCol) -> row.getAs[String](imsiCol)))
				.collectAsMap()
		val wholeMap = e2sMap ++ s2eMap
		println(wholeMap)

		//广播关系Map
		val broadcastMap = sc.broadcast(wholeMap)

		val fillValue = udf((x: String) =>
			if(broadcastMap.value.contains(x)){
				broadcastMap.value(x)
			}
			else{
				""
			}
		)
		//		df.withColumn("imei", when(df("imei").isNull, "1").otherwise("0"))
		df = df.withColumn("new_imsi", when(df(imsiCol).isNull, fillValue(df(imeiCol))).otherwise(df(imsiCol)))
				.withColumn("new_imei", when(df(imeiCol).isNull, fillValue(df(imsiCol))).otherwise(df(imeiCol)))
//		df = df.withColumn("imei", when(df("imei").isNull, fillValue(df("origimsi"))).otherwise(df("imei")))
//		df = df.withColumn("origimsi", when(df("origimsi").isNull, fillValue(df("imei"))).otherwise(df("origimsi")))
		df.show(df.count.toInt, false)

		val df2 :DataFrame = df.groupBy("new_imsi").agg(collect_set(tcbcnidCol))
				.as("tcbcnid_agg")
		df.show(df.count.toInt, false)
	}
}
