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
		//actions 和 transformations 链接在一起
		println(textFile.filter(line => line.contains("Harry")).count())
		//找到一行中最多的单词数量
		println(textFile.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b))
	}
}
