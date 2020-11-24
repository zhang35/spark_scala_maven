package apis.udaf

import basic.BaseMain

/**
 * @ClassName: TypedMyAverageAggregatorTest
 * @Description: TODO
 * @Auther: zhangjiaqi
 * @Date: 2020/11/24 15:03
 * @version : V1.0
 **/
object TypedMyAverageAggregatorTest extends BaseMain{
	override def run(): Unit = {
		import spark.implicits._
		val ds = Seq(
			("Michael", 3000),
			("Justin", 3500),
			("Andy", 4500),
			("Berta", 4000)
		).toDF("name", "salary").as[Employee]

		// Convert the function to a `TypedColumn` and give it a name
		val averageSalary = TypedMyAverageAggregator.toColumn.name("average_salary")
		val result = ds.select(averageSalary)
		result.show()
	}
}
