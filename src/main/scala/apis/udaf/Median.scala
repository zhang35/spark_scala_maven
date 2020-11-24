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
		buffer(0) = Array[Double]()
	}

	override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
		buffer(0) = buffer.getAs[Array[Double]](0) ++ input.getAs[Array[Double]](0)
	}

	override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
		buffer1(0) = buffer1.getAs[Array[Double]](0) ++ buffer2.getAs[Array[Double]](0)
	}

	override def evaluate(buffer: Row): Double = {
		val array = buffer.getAs[Array[Double]](0).sorted
		val n = array.length
		if(n % 2==0){
			(array(n/2-1) + array(n/2)) / 2.0
		}
		else{
			array(n/2)
		}
	}
}