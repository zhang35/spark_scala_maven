package apis.udaf

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

/**
 * @ClassName: typedMyAverage
 * @Description: TODO
 * @Auther: zhangjiaqi
 * @Date: 2020/11/24 14:12
 * @version : V1.0
 **/
//public abstract class Aggregator<IN,BUF,OUT>
object TypedMyAverageAggregator extends Aggregator[Employee, Average, Double] {
	// A zero value for this aggregation. Should satisfy the property that any b + zero = b
	def zero: Average = Average(0L, 0L)
	// Combine two values to produce a new value. For performance, the function may modify `buffer`
	// and return it instead of constructing a new object
	def reduce(buffer: Average, employee: Employee): Average = {
		buffer.sum += employee.salary
		buffer.count += 1
		buffer
	}
	// Merge two intermediate values
	def merge(b1: Average, b2: Average): Average = {
		b1.sum += b2.sum
		b1.count += b2.count
		b1
	}
	// Transform the output of the reduction
	def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count
	// Specifies the Encoder for the intermediate value type
	def bufferEncoder: Encoder[Average] = Encoders.product
	// Specifies the Encoder for the final output value type
	def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
