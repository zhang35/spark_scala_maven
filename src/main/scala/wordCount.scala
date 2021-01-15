import org.apache.spark.sql.SparkSession

/**
 * @ClassName: wordCount
 * @Description: TODO
 * @Auther: zhangjiaqi
 * @Date: 2020/11/19 11:52
 * @version : V1.0
 **/
object wordCount {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder()
				.master("local[*]")
				.appName("SparkExample")
				.getOrCreate()
		val sc = spark.sparkContext
		val textFile = sc.textFile("HarryPotter.txt")
		textFile.cache()
		println(textFile.count)
		println(textFile.first)
		//wordcount
		textFile.flatMap(_.split(" "))
				.map((_, 1))
				.reduceByKey(_+_)
				//按单词个数排序
				.sortBy(_._2, false)
				.collect.foreach(println)
		/*
		(,15162)
		(the,3306)
		(to,1827)
		(and,1787)
		(a,1577)
		(of,1235)
		(was,1148)
		(he,1018)
		(Harry,903)
        ...
		**/
		//actions 和 transformations 链接在一起
		println(textFile.filter(line => line.contains("Harry")).count())
		//找到一行中最多的单词数量
		println(textFile.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b))
		//停止
		sc.stop()
	}
}
