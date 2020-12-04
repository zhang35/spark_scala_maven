package algorithms.FlightTrackSegmentation.util

import java.sql.Timestamp

/**
 * @ClassName:  PointWithAttributes
 * @Description: 带属性的轨迹点，包括分段信息，速度，转角，加速度等
 * @Auther: zhangjiaqi
 * @Date: 2020/12/04
 * @version : V1.0
 **/
case class PointWithAttributes(planeID:String="", segmentID:Int=0, x:Double=.0, y:Double=.0, speed:Double=.0, accSpeed:Double=.0, turnDegree:Double=.0, timeStamp: Timestamp=new Timestamp(0), time:String="") {

  def setTime(time: String): PointWithAttributes = {
    this.copy(time=time)
  }

  def setTimeStamp(timeStamp: Timestamp): PointWithAttributes = {
    this.copy(timeStamp = timeStamp)
  }

  def setTurnDegree(turnDegree: Double): PointWithAttributes = {
    this.copy(turnDegree = turnDegree)
  }

  def setAccSpeed(accSpeed: Double): PointWithAttributes = {
    this.copy(accSpeed = accSpeed)
  }

  def setSegmentID(segmentID: Int): PointWithAttributes = {
    this.copy(segmentID=segmentID)
  }

  def setPlaneID(planeID: String): PointWithAttributes = {
    this.copy(planeID=planeID)
  }

  def setX(x: Double): PointWithAttributes = {
    this.copy(x=x)
  }

  def setY(y: Double): PointWithAttributes = {
    this.copy(y=y)
  }

  def getX: Double = x

  def getY: Double = y

  override def equals(obj: Any): Boolean = {
    var result = false

    /*if(obj.isInstanceOf[Point]){
      val point = obj.asInstanceOf[Point]
      result = point.x == this.x && point.y == this.y && (point.dateTime == this.dateTime) && (point.planeID == this.planeID)
    }*/

    //类型匹配
    obj match {
      case v: PointWithAttributes =>
        val pt: PointWithAttributes = v.asInstanceOf[PointWithAttributes]
        result = pt.x == this.x && pt.y == this.y && (pt.timeStamp  == this.timeStamp ) && (pt.planeID == this.planeID)
      case _ =>
        result = false
    }
    result
  }

  /**
    * 检测与指定点位置是否一样
    *
    * @param point 指定点
    * @return 位置一样 返回true
    */
  def samePosition(point: PointWithAttributes): Boolean = {

    point.x == this.x && point.y == this.y
  }


}
