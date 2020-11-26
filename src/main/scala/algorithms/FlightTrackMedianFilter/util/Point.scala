package algorithms.FlightTrackMedianFilter.util

import java.sql.Timestamp

/**
  * project name: zzjz-deepinsight-algorithm
  * package name: com.zzjz.deepinsight.core.Flight_Track_outliers_filter.util
  * anthor liuqiang
  * time： 2018-07-24-11-16
  * version: 
  */
class Point extends Serializable {


  var isIllegal = false

  /** *！！！为了写程序方便，将private改为public（不规范） */
  var index: Int = -1
  var angleBack = 0.0
  var angleForward = 0.0
  //    public double angleSpeed = 0.0;
  //    public double latSpeed = 0.0;
  //    public double lonSpeed = 0.0;

  //该点的平均值
  var latStatMean = 0.0
  var lonStatMean = 0.0
  //    public  double latStat_fancha = 0.0;
  //    public  double lonStat_fancha = 0.0;
  /** *与前一个点相比，经纬度前后步进值 */
  var latBackStep = 0.0
  var latForwardStep = 0.0
  var lonBackStep = 0.0
  var lonForwardStep = 0.0

  /** * 角度加速度     */
  //    public double accAngleSpeed = 0.0;
  var speed = 0.0
  //    public double accSpeed = 0.0;

  var x: Double = .0
  var y: Double = .0

//  var dateTime: DateTime = _
  var timeStamp: Timestamp = _

  /** * 飞机的 标识号     */
  var planeID: String = _

  var time: String = _


  def this(x: Double, y: Double) {
    this()
    this.x = x
    this.y = y
  }


  def setTime(time: String): Unit = {
    this.time = time
  }

  def getTime: String = time

  def setPlaneID(planeID: String): Unit = {
    this.planeID = planeID
  }

  def getPlaneID: String = this.planeID

  def setX(x: Double): Unit = {
    this.x = x
  }

  def setY(y: Double): Unit = {
    this.y = y
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
      case v: Point =>
        val pt: Point = v.asInstanceOf[Point]
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
  def samePosition(point: Point): Boolean = {

    point.x == this.x && point.y == this.y
  }


}
