package apis.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, StructField, StructType}

class Median extends UserDefinedAggregateFunction{
	override def inputSchema: StructType = StructType(
		StructField("value", DoubleType) :: Nil
	)

	override def bufferSchema: StructType = StructType(
		StructField("list", ArrayType(DoubleType)) :: Nil
	)

	override def dataType: DataType = DoubleType

	override def deterministic: Boolean = true

	override def initialize(buffer: MutableAggregationBuffer): Unit = {
		buffer(0) = Seq[Double]()
	}

	override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
		buffer(0) = buffer.getAs[Seq[Double]](0) :+ input.getAs[Double](0)
	}

	override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
		buffer1(0) = buffer1.getAs[Seq[Double]](0) ++ buffer2.getAs[Seq[Double]](0)
	}

	override def evaluate(buffer: Row): Double = {
		val wholeArray = buffer.getAs[Seq[Double]](0)
		val currentIndex = wholeArray.length / 2
		// 中值滤波，排除掉当前点，只考虑(i - window, i + window)范围内其它点
		val array = (wholeArray.slice(0, currentIndex) ++ wholeArray.slice(currentIndex+1, wholeArray.length)).sorted
		println(array)
		val n = array.length
		if((n-1) % 2==0){
			(array(n/2-1) + array(n/2)) / 2.0
		}
		else{
			array(n/2)
		}
		/*
	val a = buffer.getAs[Seq[Double]](0)
	println(a)

	WrappedArray(1.0, 2.0)
	WrappedArray(1.0, 2.0, 4.0)
	WrappedArray(2.0, 4.0, 5.0)
	WrappedArray(4.0, 5.0, 8.0)
	WrappedArray(5.0, 8.0, 9.0)
	WrappedArray(8.0, 9.0, 10.0)
	WrappedArray(9.0, 10.0, 3.0)
	WrappedArray(10.0, 3.0, 1.0)
	WrappedArray(3.0, 1.0, 2.0)
	WrappedArray(1.0, 2.0, 3.0)
	WrappedArray(2.0, 3.0, 4.0)
	WrappedArray(3.0, 4.0)	**/
	}
}