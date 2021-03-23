package tools

import basic.BaseMain
import com.jayway.jsonpath.JsonPath
/**
 * @ClassName: JSONPath
 * @Description: TODO
 * @Auther: zhangjiaqi
 * @Date: 2021/3/23 10:23
 * @version : V1.0
 **/
object JSONPath{
	def main(args: Array[String]): Unit = {
		val jsonparam = """{"types":"IMSI","userInput":"123as","MBTLTime":["2021-03-23 10:35:36","2021-03-23 10:35:38"],"area":"103.846,33.825,110.35,31.344,103.231,29.373","exceptionType":"盘旋异常","machineType":[{"title":"P8","value":"P8"},{"title":"planeC","value":"planeC"}],"RERUNNING":{"rerun":"true","preNodes":[],"nodeName":"位置MB搜索V2_1","prevInterpreters":[]}}"""
		val ctxRead = JsonPath.parse(jsonparam)

		val len = ctxRead.read[Int]("$.machineType.length()")
		for (i <- 0 until len){
			val title = ctxRead.read[String]("$.machineType[" + i + "].title")
			println(title)
		}
	}
}
