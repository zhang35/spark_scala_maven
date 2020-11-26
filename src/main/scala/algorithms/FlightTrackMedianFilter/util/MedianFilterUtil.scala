package algorithms.FlightTrackMedianFilter.util

import scala.collection.mutable.ListBuffer

//必须继承serializable，否则调用方法会报object not serializable错误
class MedianFilterUtil extends Serializable {
	/**
	 * @param ListPoint 务必要求是同一架飞机的List
	 * @param  Level    过滤条件的强弱等级, 1级最强
	 */
	def MedianFilterPoints(ListPoint: ListBuffer[Point], windowSize: Int, thres: Double): ListBuffer[Point] = {
		//去重，按时间排序
		val initialPtList: ListBuffer[Point] = ListPoint.distinct.sortBy(f => f.timeStamp.getTime)
		var cleanedPtList = new ListBuffer[Point]
		val n = initialPtList.length
		//添加第一个点
		cleanedPtList :+= initialPtList(0)
		//不修改起点和终点
		for (i <- 1 to n-2){
			val pointsInWindow = initialPtList.slice(i-windowSize, i) ++ initialPtList.slice(i+1, i+windowSize+1)
			val m = pointsInWindow.length
			val medianCoord : (Double, Double) = if (m % 2 == 0){
				( (pointsInWindow(m / 2 - 1).x + pointsInWindow(m / 2).x) / 2
						, (pointsInWindow(m / 2 - 1).y + pointsInWindow(m / 2).y) / 2 )
			}else{
				( pointsInWindow(m / 2).x, pointsInWindow(m / 2).y )
			}
//			println("medianCoord:", medianCoord)
//			println("originCoord:",initialPtList(i).x,  initialPtList(i).y)
			if (Math.abs(initialPtList(i).x - medianCoord._1) >= thres
				|| Math.abs(initialPtList(i).y - medianCoord._2) >= thres){
				println("filtering:", initialPtList(i).planeID,initialPtList(i).x, initialPtList(i).y, initialPtList(i).getTime)
				val medianPoint = initialPtList(i)
				medianPoint.setX(medianCoord._1)
				medianPoint.setY(medianCoord._2)
				cleanedPtList :+= medianPoint
			}
			else{
				cleanedPtList :+= initialPtList(i)
			}
		}
//		//少于6个点，不处理，返回原先的点
//		if (ListPoint.length >= numPointNeeded) {
//			val ptListInitialized = initializePointList(ListPoint)
//			cleanedPtList = dropOutlier(ptListInitialized, Level, interpolation)
//		} else {
//			cleanedPtList = ListPoint.sortBy(f => f.timeStamp.getTime)
//		}
		//添加最后一个点
		cleanedPtList :+= initialPtList(n-1)

		//删除连续重复坐标点，保留中间位置的点
		var retPtList = new ListBuffer[Point]
		if (cleanedPtList.length > 0){
			var i = 0
			while (i < cleanedPtList.length - 1){
				//tempBuf 保留了重复的值，比如有连续的3,3,3，则tempBuf=(3,3,3)
				var tempBuf = new ListBuffer[Point]
				while (i < (cleanedPtList.length-1)
						&& cleanedPtList(i).x == cleanedPtList(i+1).x
						&& cleanedPtList(i).y == cleanedPtList(i+1).y){
					tempBuf :+= cleanedPtList(i)
					i += 1
				}
				tempBuf :+= cleanedPtList(i)
				retPtList :+= tempBuf(tempBuf.length / 2)
				i += 1
			}
		}
		retPtList
	}
}