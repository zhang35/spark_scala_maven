/**
  * Created by liuqiang on 2018/07/17.
  */

package algorithms.FlightTrackSegmentation.util

import java.lang.Math._
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.commons.math3.geometry.euclidean.twod.Vector2D


object PointUtil extends Serializable {
  /**
    * 相邻折线段之间的夹角
    *
    * @param p1
    * @param p2
    * @param p3
    * @return
    */
  def angle(p1: PointWithSegID, p2: PointWithSegID, p3: PointWithSegID): Double = {
    var angle: Double = 0.0
    if (!p2.samePosition(p1) && !p3.samePosition(p2)) {
      //向量p1p2
      val vector1 = new Vector2D(p2.x - p1.x, p2.y - p1.y)
      //向量p2p3
      val vector2 = new Vector2D(p3.x - p2.x, p3.y - p2.y)

      angle = Vector2D.angle(vector1, vector2)

      if (crossProduct(p1, p2, p3) < 0)
        angle = -angle
    }
    angle
  }

  /**
    * p12 与  p23  的叉乘（向量积）
    * 为正时，p1-p2-p3   路径的走向为逆时针，
    * 为负时，p1-p2-p3   走向为顺时针，
    * 为零时，p1-p2-p3   所走的方向不变，亦即三点在一直线上。
    *
    * @param p1
    * @param p2
    * @param p3
    * @return
    */
  def crossProduct(p1: PointWithSegID, p2: PointWithSegID, p3: PointWithSegID): Double = {
    val crossProduct = (p2.x - p1.x) * (p3.y - p2.y) - (p2.y - p1.y) * (p3.x - p2.x)
    crossProduct
  }

  /**
    * 相邻折线段之间的夹角 的角速度
    *
    * @param p1
    * @param p2
    * @param p3
    * @return
    */
  def angleSpeed(p1: PointWithSegID, p2: PointWithSegID, p3: PointWithSegID): Double = {
    var angleSpeed = 0.0
    if (!p2.samePosition(p1) && !p3.samePosition(p2)) {
      //向量p1p2
      val vector1 = new Vector2D(p2.x - p1.x, p2.y - p1.y)
      //向量p2p3
      val vector2 = new Vector2D(p3.x - p2.x, p3.y - p2.y)
      val ti = abs(basicTools.durationSeconds(p2.timeStamp, p3.timeStamp))
      val ai = Vector2D.angle(vector1, vector2)
      if (ti == 0) {
        if (ai > 0)
          angleSpeed = Double.PositiveInfinity
        if (ai < 0)
          angleSpeed = Double.NegativeInfinity
      } else {
        angleSpeed = ai / ti
      }
    }
    angleSpeed
  }

  def accAngleSpeed(p1: PointWithSegID, p2: PointWithSegID, p3: PointWithSegID, p4: PointWithSegID): Double = {
    var angleAccSpeed = 0.0
    val angleSpeed1 = angleSpeed(p1, p2, p3)
    val angleSpeed2 = angleSpeed(p2, p3, p4)
    val ai = angleSpeed2 - angleSpeed1

    val ti = abs(basicTools.durationSeconds(p2.timeStamp, p4.timeStamp))
    if (ti == 0) {
      if (ai > 0)
        angleAccSpeed = Double.PositiveInfinity
      if (ai < 0)
        angleAccSpeed = Double.NegativeInfinity
    } else {
      angleAccSpeed = ai / ti
    }
    angleAccSpeed
  }


  /**
    * 速度是经纬度两个方向分速度的矢量合成，因此考虑使用经度方向的速度作为判定依据，
    * 目的是避免计算空间距离，从而减少了计算量
    *
    * @param p1
    * @param p2
    * @return
    */
  def latSpeed(p1: PointWithSegID, p2: PointWithSegID): Double = {
    var latSpeed = 0.0
    if (!p1.samePosition(p2)) {
      val ti = abs(basicTools.durationSeconds(p1.timeStamp, p2.timeStamp))
      val di = math.abs(distanceOfLatitude(p1, p2)) * 111000

      if (ti == 0) {
        latSpeed = Double.PositiveInfinity
      } else {
        latSpeed = di / ti
      }
    }
    latSpeed
  }

  /**
    * 每1°经度地表面的实地长度大约就是111千米,但是飞机航迹采样间隔小（秒级），一般不会出现较大的经纬度跨越。
    * 因此适当放大一定倍数，表征速度变化即可
    *
    * @param p1
    * @param p2
    * @return
    */

  def lonSpeed(p1: PointWithSegID, p2: PointWithSegID): Double = {
    var lonSpeed = 0.0
    if (!p1.samePosition(p2)) {
      val ti = abs(basicTools.durationSeconds(p1.timeStamp, p2.timeStamp))
      val di = math.abs(distanceOfLongitude(p1, p2)) * 111000
      if (ti == 0) {
        lonSpeed = Double.PositiveInfinity
      } else if (ti != 0) {
        lonSpeed = di / ti
      }
    }
    lonSpeed
  }


  /**
    * 求两点之间平面坐标的长度
    *
    * @param   p1 点1
    * @param   p2 点2
    * @return 两点之间的距离
    */
  def lengthOf2point(p1: PointWithSegID, p2: PointWithSegID): Double = {
    var length = 0.0

    if (!p1.samePosition(p2) && p1 != null && p2 != null) {
      val a = Math.abs(p1.getX - p2.getX)
      // 直角三角形的直边a
      val b = Math.abs(p1.getY - p2.getY)
      // 直角三角形的直边b
      val min = Math.min(a, b)
      // 短直边
      val max = Math.max(a, b) // 长直边
      /**
        * 为防止计算平方时float溢出，做如下转换
        * √(min²+max²) = √((min/max)²+1) * abs(max)
        */
      val inner = min / max
      length = Math.sqrt(inner * inner + 1.0) * max
    }
    length
  }

  /**
    * 求两点间的中心点
    *
    * @param   p1 点1
    * @param   p2 点2
    * @return 两点间的中心点
    */
  def getCenterPoint(p1: PointWithSegID, p2: PointWithSegID): PointWithSegID = {
    if (null == p1 || null == p2) return null
    new PointWithSegID(p1.getX + (p2.getX - p1.getX) / 2.0, p1.getY + (p2.getY - p1.getY) / 2.0)
  }

  def predictNextPoint(p1: PointWithSegID, p2: PointWithSegID): PointWithSegID = {
    if (null == p1 || null == p2) return null
    val x = p2.getX + (p2.getX - p1.getX)
    val y = p2.getY + (p2.getY - p1.getY)
    new PointWithSegID(x, y)
  }

  /**
    * <p>
    * <b>计算向量角</b>
    * <p>
    * <pre>
    * 计算两点组成的向量与x轴正方向的向量角
    * </pre>
    *
    * @param   s 向量起点
    * @param   d 向量终点
    * @return 向量角
    */
  def angleOfOrdinate(s: PointWithSegID, d: PointWithSegID): Double = {
    var angle = 0.0
    val dist = lengthOf2point(s, d)
    if (dist > 0) {
      val x = d.getX - s.getX
      val y = d.getY - s.getY
      /* 1 2 象限 */
      if (y >= 0)
        Math.acos(x / dist)
      else /* 3 4 象限 */
        angle = Math.acos(-x / dist) + Math.PI
    }
    angle
  }

  /**
    * <p>
    * <b>修正角度</b>
    * <p>
    * <pre>
    * 修正角度到 [0, 2PI]
    * </pre>
    *
    * @param   Angle 原始角度
    * @return 修正后的角度
    */
  def reviseAngle(Angle: Double): Double = {
    var angle = Angle
    while (angle < 0)
      angle += 2 * Math.PI
    while (angle >= 2 * Math.PI)
      angle -= 2 * Math.PI
    angle
  }


  /**
    * 角度转弧度
    *
    * @param degree 角度
    * @return
    */
  def convertDegreesToRadians(degree: Double): Double = {
    (degree * PI) / 180
  }


  /**
    * 经度方向上的距离投影
    *
    * @param p1
    * @param p2
    * @return
    */
  def distanceOfLongitude(p1: PointWithSegID, p2: PointWithSegID): Double = {
    val xDistance: Double = Math.asin(Math.sin((p2.x - p1.x) * Math.PI / 180)) * 180 / Math.PI // p2.x - p1.x
    xDistance
  }

  /**
    * 纬度方向上的距离投影
    *
    * @param p1
    * @param p2
    * @return
    */
  def distanceOfLatitude(p1: PointWithSegID, p2: PointWithSegID): Double = {
    val yDistance: Double = Math.asin(Math.sin((p2.y - p1.y) * Math.PI / 180)) * 180 / Math.PI //p2.y - p1.y
    yDistance
  }




  def getTimestamp(str: String): Timestamp = {
    val regex1 = "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{1,3}" //2018-07-27 18:31:18.000
    val regex2 = "[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{1,3}" //2018/07/27 18:31:18.000
    val regex3 = "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}" //2018-07-27 18:31:18
    val regex4 = "[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}" //2018/07/27 18:31:18
    try {
      if (str.matches(regex1)) {
        val dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        val lt = dataFormat.parse(str).getTime
        new java.sql.Timestamp(lt)
      } else if (str.matches(regex2)) {
        val dataFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS")
        val lt = dataFormat.parse(str).getTime
        new java.sql.Timestamp(lt)
      } else if (str.matches(regex3)) {
        val dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val lt = dataFormat.parse(str).getTime
        new java.sql.Timestamp(lt)
      } else if (str.matches(regex4)) {
        val dataFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
        val lt = dataFormat.parse(str).getTime
        new java.sql.Timestamp(lt)
      }
      else {
        null
      }
    }
    catch {
      case _: Exception => null
    }
  }


}
