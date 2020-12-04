package algorithms.FlightTrackSegmentation.util

import scala.collection.mutable.ListBuffer

//必须继承serializable，否则调用方法会报object not serializable错误
class FlightTrackSegmentationUtil extends Serializable {
	/**
	 * @param ListPoint 务必要求是同一架飞机的List
	 * @param method 分段依据；0：转角；1：加速度
	 * @param turnDegreeThres 转角阈值
	 * @param accSpeedThres 加速度阈值
	 */
	def SegmentPoints(ListPoint: ListBuffer[PointWithAttributes], method:Integer, turnDegreeThres: Double = .0, accSpeedThres: Double = .0): ListBuffer[PointWithAttributes] = {
		// 按时间排序
		val initialPtList: ListBuffer[PointWithAttributes] = ListPoint.distinct.sortBy(f => f.timeStamp.getTime)
		var retPtList = new ListBuffer[PointWithAttributes]
		val n = initialPtList.length
		if (n <= 2 || turnDegreeThres > 180 || turnDegreeThres < 0){
			initialPtList
		}
		else{
			var p1 = initialPtList(0)
			var p2 = initialPtList(1)
			//得到时刻,单位ms
			val t1 = p1.timeStamp.getTime
			var t2 = p2.timeStamp.getTime
			val d1 = PointUtil.distanceOf2point(p2, p1)
			//速度单位m/s
			var v1 = if (t2 != t1){
				d1 * 1000.0 / (t2 - t1)
			}else{
				0.0
			}
			//轨迹分段标号
			var id = 1
			//加入起点
			retPtList :+= p1.setSegmentID(id)

			for (i <- 2 until n){
				val p3 = initialPtList(i)
				//val degree = PointUtil.angle(p1, p2, p3) / math.Pi * 360.0
				// 计算转角
				val degree = PointUtil.degree(p1, p2, p3)

				//计算加速度,单位m/s^2
				val t3 = p3.timeStamp.getTime
				val d2 = PointUtil.distanceOf2point(p3, p2)
				val v2 = if (t3 != t2){
					d2 * 1000.0 / (t3 - t2)
				}else{
					0.0
				}
				val acc = if(t3 != t2){
					(v2 - v1) * 1000.0 / (t3 - t2)
				}
				else{
					0.0
				}

				//发现新分段，更新分段号
				if ((method==0 && Math.abs(degree) >= turnDegreeThres)
				|| (method==1 && Math.abs(acc) >= accSpeedThres)){
					//上一段的终点
					retPtList :+= p2.copy(segmentID = id, accSpeed = acc, turnDegree = degree)
					println("seperate!", degree, acc)
					//下一段的起点
					id += 1
					retPtList :+= p2.setSegmentID(id)
				}
				else{
					retPtList :+= p2.copy(segmentID = id, accSpeed = acc, turnDegree = degree)
				}

				//滑动窗口
				p1 = p2
				p2 = p3
				v1 = v2
				t2 = t3

			}
			//加入终点
			retPtList :+= initialPtList(n-1).setSegmentID(id)
			retPtList
		}
	}
}