package algorithms

import basic.BaseMain
import com.google.gson.JsonParser
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.when

/**
 * @ClassName: test
 * @Description: TODO
 * @Auther: zhangjiaqi
 * @Date: 2020/11/19 10:44
 * @version : V1.0
 **/
object test extends BaseMain {
	override def run(): Unit = {
		val jsonparam = "<#zzjzParam#>"
//		val jsonparam = """{"RERUNNING":{"nodeName":"轨迹标号对齐_1","preNodes":[{"checked":true,"id":"读关系型数据库_1_nP982jUf"}],"prevInterpreters":[{"interpreter":"ZZJZ-Algorithm","paragraphId":"读关系型数据库_1_nP982jUf"}],"rerun":"false"},"imeiCol":"lat","imsiCol":"id","inputTableName":"读关系型数据库_1_nP982jUf","tcbcnidCol":"long"}"""
		println(jsonparam)

		val parser = new JsonParser();
		val p = parser.parse(jsonparam).getAsJsonObject

		// 获取输入参数
		val inputTableName = p.get("inputTableName").getAsString
		val imsiCol = p.get("imsiCol").getAsString
		val imeiCol = p.get("imeiCol").getAsString
		val tcbcnidCol = p.get("tcbcnidCol").getAsString

		// 获取输入DataFrame
		var df : DataFrame = z.rdd(inputTableName).asInstanceOf[org.apache.spark.sql.DataFrame]
		df.printSchema()

		df = df.withColumn("origimsi",
			when(df(imsiCol).isNull && df(tcbcnidCol)<=30, "000")
        			.when(df(imsiCol).isNull && df(tcbcnidCol)>30 && df(tcbcnidCol)<=44, "111")
					.when(df(imsiCol).isNull && df(tcbcnidCol)>44 && df(tcbcnidCol)<=68, "222")
					.when(df(imsiCol).isNull && df(tcbcnidCol)>68 && df(tcbcnidCol)<=89, "333")
					.when(df(imsiCol).isNull && df(tcbcnidCol)>89, "444")
					.otherwise(df(imsiCol)))
		df.show(df.count.toInt, false)

//		//找出关联列
//		val relationDf = df.filter(df(imsiCol).isNotNull && df(imeiCol).isNotNull)
//				.dropDuplicates(imsiCol, imeiCol)
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
//
//		//		df = df.withColumn("imei", when(df("imei").isNull, fillValue(df("origimsi"))).otherwise(df("imei")))
//		//		df = df.withColumn("origimsi", when(df("origimsi").isNull, fillValue(df("imei"))).otherwise(df("origimsi")))
//
//		// 统计用户号对应的所有临时号
//		val df2 :DataFrame = df.groupBy("new_imsi").agg(collect_set(tcbcnidCol))
//				.as("tcbcnid_agg")
//		df2.show
//
//		// 输出表1
//		val rddTableName1 = "<#zzjzRddName#>_completion"
//		outputrdd.put(rddTableName1, df)
//		df.registerTempTable(s"`${rddTableName1}`")
//		df.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)
//		sqlc.cacheTable(s"`${rddTableName1}`")
//
//		// 输出表2
//		val rddTableName2 = "<#zzjzRddName#>_tcbcnid_agg"
//		outputrdd.put(rddTableName2, df2)
//		df.registerTempTable(s"`${rddTableName2}`")
//		df.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)
//		sqlc.cacheTable(s"`${rddTableName2}`")
	}
}
