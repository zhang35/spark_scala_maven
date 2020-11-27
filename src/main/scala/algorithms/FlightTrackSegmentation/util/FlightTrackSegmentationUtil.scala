package algorithms.FlightTrackSegmentation.util

import scala.collection.mutable.ListBuffer

//必须继承serializable，否则调用方法会报object not serializable错误
class FlightTrackSegmentationUtil extends Serializable {
	/**
	 * @param ListPoint 务必要求是同一架飞机的List
	 * @param  Level    过滤条件的强弱等级, 1级最强
	 */
	def SegmentPoints(ListPoint: ListBuffer[PointWithSegID], turnDegreeThres: Double, accSpeedThres: Double): ListBuffer[PointWithSegID] = {
		var retPtList = new ListBuffer[PointWithSegID]
		val n = ListPoint.length
		println(n, turnDegreeThres)
		if (n <= 2 || turnDegreeThres > 180 || turnDegreeThres < 0){
			ListPoint
		}
		else{
			var p1 = ListPoint(0)
			var p2 = ListPoint(1)
			for (i <- 2 until n){
				var p3 = ListPoint(i)
//				val degree = PointUtil.angle(p1, p2, p3) / math.Pi * 360.0
				val degree = PointUtil.angle(p1, p2, p3)
				println(p3.planeID, i-2, i-1, i, degree)
				p1 = p2
				p2 = p3
			}

			retPtList
		}
	}
}