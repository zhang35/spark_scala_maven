package algorithms.imsiAlignment
import org.apache.spark.Partitioner
class ColumnPartitioner(val num: Int) extends Partitioner{
	override def numPartitions: Int = num
	override def getPartition(key: Any): Int = {
		key.toString.toInt % num
	}
}
