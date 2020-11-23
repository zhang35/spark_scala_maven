import basic.BaseMain

/**
 * @ClassName: wordCount
 * @Description: TODO
 * @Auther: zhangjiaqi
 * @Date: 2020/11/19 11:52
 * @version : V1.0
 **/
object wordCount extends BaseMain{
	override def run(): Unit = {
		val textFile = sc.textFile("E:\\HarryPotter.txt")
		textFile.cache()
		println(textFile.count)
		println(textFile.first)
		//actions 和 transformations 链接在一起
		println(textFile.filter(line => line.contains("Harry")).count())
		//找到一行中最多的单词数量
		println(textFile.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b))
	}
}
