package dataSource

import java.util.Properties

import org.apache.spark.sql.{SQLContext, SparkSession}

/**
 * @ClassName: readJDBC
 * @Description: spark读jdbc数据源的两种方法
 * @Auther: zhangjiaqi
 * @Date: 2020/11/19 10:24
 * @version : V1.0
 **/

/**
需要在maven中配置mysql-connector-java包依赖:
*               <dependency>
*               <groupId>mysql</groupId>
*               <artifactId>mysql-connector-java</artifactId>
*               <version>5.1.47</version>
*               </dependency>
                 **/

object readJDBC {
	def main(args: Array[String]): Unit = {

		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()

		//方法一: 使用sparkSession万能格式读取
		val df1 = spark.read.format("jdbc")
				.option("url", "jdbc:mysql://192.168.11.26:3306/test_zjq")
				.option("driver","com.mysql.jdbc.Driver")
				.option("dbtable", "gps_points1")
				.option("user", "oozie")
				.option("password", "oozie")
				.load()
		df1.show

		//方法二: 使用sqlContext读取
		val properties = new Properties()
		properties.put("user","oozie")
		properties.put("password","oozie")
		val url = "jdbc:mysql://192.168.11.26:3306/test_zjq"
		lazy val sqlc: SQLContext =spark.sqlContext
		val df2 = sqlc.read.jdbc(url,"gps_points1",properties)
		df2.show
	}
}
