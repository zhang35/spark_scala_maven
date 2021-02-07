package dataSource

import com.google.gson.Gson
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @ClassName: httpGet
 * @Description: TODO
 * @Auther: zhangjiaqi
 * @Date: 2021/2/7 10:35
 * @version : V1.0
 **/
object httpGet {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()

		def GetUrlContentJson(url: String): DataFrame ={
			val result = scala.io.Source.fromURL(url).mkString
			//only one line inputs are accepted. (I tested it with a complex Json and it worked)
			val jsonResponseOneLine = result.toString().stripLineEnd
			//You need an RDD to read it with spark.read.json! This took me some time. However it seems obvious now
			val jsonRdd = spark.sparkContext.parallelize(jsonResponseOneLine :: Nil)

			val jsonDf = spark.read.json(jsonRdd)
			return jsonDf
		}


		val url = "http://192.168.11.109:5000/lastappeared?date=2020-11-17"
		val outDf = GetUrlContentJson(url)

		outDf.show

	}
}
