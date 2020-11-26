package algorithms.FlightTrackMedianFilter.util

import java.lang.Math._
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.commons.math3.geometry.euclidean.twod.Vector2D
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * +===========================
  * 提供轨迹分析需要的常用方法
  * 提醒：gps经纬度有正负的，北纬是正，南纬是负，东经是正，西经是负
  * +============================
  */

object basicTools {


  /**
    * 字符串转为DateTime类型的时间
    *
    * @param str 字符串形式的时间 ，仅支持四种格式的时间
    * @return 返回org.joda.time.DateTime类型的时间
    */
  def convertDateTime(str: String): DateTime = {
    val regex1 = "[1-9]\\d{3}-(0[1-9]|1[0-2]|[1-9])-(0[1-9]|[1-2][0-9]|3[0-1]|[1-9])\\s+" +
      "(2[0-3]|[0-1]\\d|\\d):([0-5]\\d|\\d):([0-5]\\d|\\d).\\d{1,3}" //2018-07-27 18:31:18.[f..]
    val regex2 = "[1-9]\\d{3}/(0[1-9]|1[0-2]|[1-9])/(0[1-9]|[1-2][0-9]|3[0-1]|[1-9])\\s+" +
      "(2[0-3]|[0-1]\\d|\\d):([0-5]\\d|\\d):([0-5]\\d|\\d).\\d{1,3}" //2018/07/27 18:31:18.[f..]
    val regex3 = "[1-9]\\d{3}-(0[1-9]|1[0-2]|[1-9])-(0[1-9]|[1-2][0-9]|3[0-1]|[1-9])\\s+" +
      "(2[0-3]|[0-1]\\d|[0-9]):([0-5]\\d|[0-9]):([0-5]\\d$|\\d$)" //2018-07-27 18:31:18
    val regex4 = "[1-9]\\d{3}/(0[1-9]|1[0-2]|[1-9])/(0[1-9]|[1-2][0-9]|3[0-1]|[1-9])\\s+" +
      "(2[0-3]|[0-1]\\d|[0-9]):([0-5]\\d|[0-9]):([0-5]\\d$|\\d$)" //2018/07/27 18:31:18

    try {
      if (str.matches(regex1)) {
        DateTime.parse(str.trim, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS"))
      } else if (str.matches(regex2)) {
        DateTime.parse(str.trim, DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss.SSS"))
      } else if (str.matches(regex3)) {
        DateTime.parse(str.trim, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
      } else if (str.matches(regex4)) {
        DateTime.parse(str.trim, DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss"))
      }
      else {
        null
      }
    }
    catch {
      case _: Exception => null
    }
  }


  /**
    * 字符串转为Timestamp类型的时间
    * 要求字符串格式为： yyyy-mm-dd hh:mm:ss[.f...] 或者 yyyy/MM/dd HH:mm:ss[.f...] 这样的格式，中括号表示可选，否则返回null
    *
    * @param strTime 字符串形式的时间
    * @return
    */
  def convertTimestamp(strTime: String): Timestamp = {
    val regex1 = "[1-9]\\d{3}/(0[1-9]|1[0-2]|[1-9])/(0[1-9]|[1-2][0-9]|3[0-1]|[1-9])\\s+" +
      "(2[0-3]|[0-1]\\d|[0-9]):([0-5]\\d|[0-9]):([0-5]\\d$|\\d$)" //2018/07/27 18:31:18
    try {
      if (strTime.contains('-')) {
        Timestamp.valueOf(strTime)
      } else {
        // 某些应用场景，小数点后的位数不确定，因此只取小数点前的部分
        // note：精确度到秒，忽略了毫秒部分
        val str: String = strTime.split("\\.")(0)
        if (str.matches(regex1)) {
          val dataFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
          val lt = dataFormat.parse(str).getTime
          new java.sql.Timestamp(lt)
        }
        else {
          null
        }
      }
    }
    catch {
      case _: Exception => null
    }
  }


  /**
    * 计算gps两点在经度方向上移动的间隙, 经度方向上的距离投影
    * 要保证180°经线前后依然按照由东向西为正且大小一致
    * asin { sin(x2 - x1) } --外加(x2 - x1)的弧度转换
    *
    * @param p1 出发点
    * @param p2 到达点
    * @return 经度方向上移动的度数
    */
  def longitudeInterval(p1: Point, p2: Point): Double = {
    val xInterval = p2.x - p1.x
    Math.asin(Math.sin(xInterval * Math.PI / 180)) * 180 / Math.PI
  }

  /**
    * 计算gps两点在纬度方向上移动的间隙, 纬度方向上的距离投影
    * 南极，北极附近区域会不准确，尤其是跨越南极北极的情况。
    * 如果跨越南极北极，正负方向将不存在（北纬是正，南纬是负）
    *
    * @param p1 出发点
    * @param p2 到达点
    * @return 纬度方向上移动的度数y
    */
  def latitudeInterval(p1: Point, p2: Point): Double = {
    var yInterval = p2.y - p1.y
    //穿过南极北极情况，经度会突变，这里判定的数值都是估算
    if (math.abs(p2.x - p1.x) > 160) {
      //北极附近,北纬是正
      if (90 - p1.y < 0.5 || 90 - p2.y < 0.5) yInterval = 180 - p1.y - p2.y //90 - p1.y + 90 - p2.y
      //南极附近,南纬是负
      if (90 + p1.y < 0.5 || 90 + p2.y < 0.5) yInterval = 180 + p1.y + p2.y //90 + p1.y + 90 + p2.y
    }
    yInterval
  }


  /**
    * 相邻折线段之间的夹角，弧度值
    * 为正时，p1-p2-p3   路径的走向为逆时针，
    * 为负时，p1-p2-p3   走向为顺时针，
    *
    * @param p1
    * @param p2
    * @param p3
    * @return 折线p1p2和p2p3 在点p2处的夹角，弧度值。逆时针为正，顺时针为负
    */
  def angle(p1: Point, p2: Point, p3: Point): Double = {
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
    /*if (!p2.samePosition(p1) && !p3.samePosition(p2)) {
      //向量p1p2
      val vector1 = new myVector2D(p1, p2)
      //向量p2p3
      val vector2 = new myVector2D(p2, p3)

      angle = new myVector2D(p1, p3).radianBetweenByDot(vector1, vector2)
    }*/
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
  def crossProduct(p1: Point, p2: Point, p3: Point): Double = {
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
  def angleSpeed(p1: Point, p2: Point, p3: Point): Double = {
    var angleSpeed = 0.0
    if (!p2.samePosition(p1) && !p3.samePosition(p2)) {
      //向量p1p2
      val vector1 = new Vector2D(p2.x - p1.x, p2.y - p1.y)
      //向量p2p3
      val vector2 = new Vector2D(p3.x - p2.x, p3.y - p2.y)
      val ti = abs(durationSeconds(p2.timeStamp, p3.timeStamp))
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

  def accAngleSpeed(p1: Point, p2: Point, p3: Point, p4: Point): Double = {
    var angleAccSpeed = 0.0
    val angleSpeed1 = angleSpeed(p1, p2, p3)
    val angleSpeed2 = angleSpeed(p2, p3, p4)
    val ai = angleSpeed2 - angleSpeed1

    val ti = abs(durationSeconds(p2.timeStamp, p4.timeStamp))
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
  def latSpeed(p1: Point, p2: Point): Double = {
    var latSpeed = 0.0
    if (!p1.samePosition(p2)) {
      val ti = abs(durationSeconds(p1.timeStamp, p2.timeStamp))
      val di = math.abs(latitudeInterval(p1, p2)) * 111000

      if (ti == 0) {
        latSpeed = Double.PositiveInfinity
      } else {
        latSpeed = di / ti
      }
    }
    latSpeed
  }

  /**
    * 每1°经度地表面的实地长度大约就是111千米,但是飞机航迹采样间隔小（一般秒级,相对于速度来说。），一般不会出现较大的经纬度跨越。
    * 因此适当放大一定倍数，表征速度变化即可
    *
    * @param p1
    * @param p2
    * @return
    */
  def lonSpeed(p1: Point, p2: Point): Double = {
    var lonSpeed = 0.0
    if (!p1.samePosition(p2)) {
      val ti = abs(durationSeconds(p1.timeStamp, p2.timeStamp))
      val di = math.abs(longitudeInterval(p1, p2)) * 111000
      if (ti == 0) {
        lonSpeed = Double.PositiveInfinity
      } else if (ti != 0) {
        lonSpeed = di / ti
      }
    }
    lonSpeed
  }

  /**
    * 计算时间t2和t2之间的间隔（t2-t1，单位：秒）
    *
    * @param startTime 起始时间
    * @param endTime   结束时间
    * @return 时间间隔，单位：秒
    */
  def durationSeconds(startTime: Timestamp, endTime: Timestamp) = {
    (endTime.getTime - startTime.getTime) / 1000L
  }

  /**
    * 计算时间t2和t2之间的间隔（t2-t1，单位：分钟）
    *
    * @param startTime 起始时间
    * @param endTime   结束时间
    * @return 时间间隔，单位：分钟
    */
  def durationMinutes(startTime: Timestamp, endTime: Timestamp) = {
    (endTime.getTime - startTime.getTime) / 60000L
  }

  /**
    * 计算时间t2和t2之间的间隔（t2-t1，单位：小时）
    *
    * @param startTime 起始时间
    * @param endTime   结束时间
    * @return 时间间隔，单位：小时
    */
  def durationHours(startTime: Timestamp, endTime: Timestamp) = {
    (endTime.getTime - startTime.getTime) / 3600000L
  }

  /**
    * 计算时间t2和t2之间的间隔（t2-t1，单位：天）
    *
    * @param startTime 起始时间
    * @param endTime   结束时间
    * @return 时间间隔，单位：天
    */
  def durationDays(startTime: Timestamp, endTime: Timestamp) = {
    (endTime.getTime - startTime.getTime) / 86400000L
  }

  /**
    * 计算时间t2和t2之间的间隔（t2-t1，单位：天）
    *
    * @param startTime 起始时间
    * @param endTime   结束时间
    * @return 时间间隔，单位：周
    */
  def durationWeeks(startTime: Timestamp, endTime: Timestamp) = {
    (endTime.getTime - startTime.getTime) / 604800000L
  }

  /**
    * 计算时间t2和t2之间的间隔（t2-t1，单位：天）
    *
    * @param startTime 起始时间
    * @param endTime   结束时间
    * @return 时间间隔，单位：month
    */
  def durationMonths(startTime: Timestamp, endTime: Timestamp) = {
    (endTime.getTime - startTime.getTime) / 2592000000L
  }

  /**
    * 计算时间t2和t2之间的间隔（t2-t1，单位：天）
    *
    * @param startTime 起始时间
    * @param endTime   结束时间
    * @return 时间间隔，单位：year
    */
  def durationYears(startTime: Timestamp, endTime: Timestamp) = {
    (endTime.getTime - startTime.getTime) / 31536000000L
  }


  /**
    * 求两点之间的平面坐标长度  √(x²+y²)
    *
    * @param   p1 点1
    * @param   p2 点2
    * @return 两点之间的距离 √(x²+y²)
    */
  def lengthOf2point(p1: Point, p2: Point): Double = {
    var length = 0.0

    if (!p1.samePosition(p2) && p1 != null && p2 != null) {
      val a = Math.abs(p1.getX - p2.getX)
      // 直角三角形的直边a
      val b = Math.abs(p1.getY - p2.getY)
      // 直角三角形的直边b
      val min = Math.min(a, b) // 短直边

      val max = Math.max(a, b) // 长直边
      /**
        * 为防止计算平方时double溢出，做如下转换
        * √(min²+max²) = √((min/max)²+1) * abs(max)
        */
      val inner = min / max
      length = Math.sqrt(inner * inner + 1.0) * max
    }
    length
  }


  /*lazy val distanceOf2point2 = (lat1: Double, lon1: Double, lat2: Double, lon2: Double) => {
    //pi为π，r为地球半径
    val pi = 3.1415926
    val r: Double = 6371 //6370996.81 //6370.99681
    //a1、a2、b1、b2分别为上面数据的经纬度转换为弧度
    val a1 = lat1 * pi / 180.0
    val a2 = lon1 * pi / 180.0
    val b1 = lat2 * pi / 180.0
    val b2 = lon2 * pi / 180.0
    val t1: Double = Math.cos(a1) * Math.cos(a2) * Math.cos(b1) * Math.cos(b2)
    val t2: Double = Math.cos(a1) * Math.sin(a2) * Math.cos(b1) * Math.sin(b2)
    val t3: Double = Math.sin(a1) * Math.sin(b1)
    val distance = Math.acos(t1 + t2 + t3) * r
    distance
  }*/

  lazy val convertDegreesToRadians = (degree: Double) => (degree * PI) / 180
  lazy val haversin = (x: Double) => {
    val s = sin(x / 2)
    s * s
  }

//  地球半径，单位：千米
  val EARTH_RADIUS =  6378.137  // 6370.99681

  /**
    * 计算地理坐标经纬度两点的距离 单位： 千米
    *
    * @param lat1
    * @param lon1
    * @param lat2
    * @param lon2
    * @return 单位： 千米
    */
  def distanceOf2point_II(xLat: Double, xLon: Double, yLat: Double, yLon: Double): Double = {
    val xlat = convertDegreesToRadians(xLat)
    val xlon = convertDegreesToRadians(xLon)
    val ylat = convertDegreesToRadians(yLat)
    val ylon = convertDegreesToRadians(yLon)

    val vlon = abs(xlon - ylon)
    val vlat = abs(xlat - ylat)

    val h = haversin(vlat) + cos(xlat) * cos(ylat) * haversin(vlon)
    val dis = 2 * EARTH_RADIUS * asin(sqrt(h))
    dis
  }

  /**
    * 计算地理坐标经纬度两点的距离 单位：千米
    * (该方法由GOOGLE提供，误差小于0.2米)
    *
    * @param lat1
    * @param lon1
    * @param lat2
    * @param lon2
    * @return 单位： 千米
    */
  def distanceOf2point(xLat: Double, xLon: Double, yLat: Double, yLon: Double): Double = {
    //    val EARTH_RADIUS = 6378.137
    val radLat1 = convertDegreesToRadians(xLat)
    val radLng1 = convertDegreesToRadians(xLon)
    val radLat2 = convertDegreesToRadians(yLat)
    val radLng2 = convertDegreesToRadians(yLon)
    val a = radLat1 - radLat2
    val b = radLng1 - radLng2


    val distance = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2)
      + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2))) * EARTH_RADIUS
    distance
  }

  /**
    * 求两点间的中心点
    *
    * @param   p1 点1
    * @param   p2 点2
    * @return 两点间的中心点
    */
  def getCenterPoint(p1: Point, p2: Point): Point = {
    if (null == p1 || null == p2) return null
    new Point(p1.getX + (p2.getX - p1.getX) / 2.0, p1.getY + (p2.getY - p1.getY) / 2.0)
  }

  /**
    * 根据p1，p2点，线性预测下一个点p3
    * p2为中间点
    *
    * @param p1
    * @param p2
    * @return 点p3
    */
  def predictNextPoint(p1: Point, p2: Point): Point = {
    if (null == p1 || null == p2) return null
    val x = p2.getX + (p2.getX - p1.getX)
    val y = p2.getY + (p2.getY - p1.getY)
    new Point(x, y)
  }

  /**
    * 计算两点组成的向量与x轴正方向的向量角
    *
    * @param   s 向量起点
    * @param   d 向量终点
    * @return 向量角
    */
  def angleOfOrdinate(s: Point, d: Point): Double = {
    var angle = 0.0
    val dist = lengthOf2point(s, d)
    if (dist > 0) {
      val x = d.getX - s.getX
      val y = d.getY - s.getY
      /* 1 2 象限 */
      if (y >= 0)
        angle = Math.acos(x / dist)
      else /* 3 4 象限 */
        angle = Math.acos(-x / dist) + Math.PI
    }
    angle
  }

  /**
    * 修正角度到 [0, 2PI]， 弧度值
    *
    * @param   Radian 原始角度
    * @return 修正后的角度， 弧度值[0, 2PI]
    */
  def LimitAngle0To2Pi(Radian: Double): Double = {
    var radian = Radian
    while (radian < 0)
      radian += 2 * Math.PI
    while (radian >= 2 * Math.PI)
      radian -= 2 * Math.PI
    radian
  }


  /**
    * 角度转弧度
    *
    * @param Angle 角度
    * @return 弧度
    */
  def convertAngleToRadian(Angle: Double): Double = {
    (Angle * PI) / 180
  }

  /**
    * 弧度转角度
    *
    * @param Radian
    * @return 角度
    */
  def convertRadianToAngle(Radian: Double): Double = {
    (Radian * 180) / PI
  }

  /**
    * 判断是否为合法的时间格式（1970年以后的时间，最晚不超过2029年）
    *
    * @param str 时间字符串
    * @return Boolean
    */
  def isTimeString(str: String): Boolean = {
   /* val regex1 = "[1-9]\\d{3}-(0[1-9]|1[0-2]|[1-9])-(0[1-9]|[1-2][0-9]|3[0-1]|[1-9])\\s+" +
      "(2[0-3]|[0-1]\\d|\\d):([0-5]\\d|\\d):([0-5]\\d|\\d).\\d{1,3}" //2018-07-27 18:31:18.[f..]
    val regex2 = "[1-9]\\d{3}/(0[1-9]|1[0-2]|[1-9])/(0[1-9]|[1-2][0-9]|3[0-1]|[1-9])\\s+" +
      "(2[0-3]|[0-1]\\d|\\d):([0-5]\\d|\\d):([0-5]\\d|\\d).\\d{1,3}" //2018/07/27 18:31:18.[f..]
    val regex3 = "[1-9]\\d{3}-(0[1-9]|1[0-2]|[1-9])-(0[1-9]|[1-2][0-9]|3[0-1]|[1-9])\\s+" +
      "(2[0-3]|[0-1]\\d|[0-9]):([0-5]\\d|[0-9]):([0-5]\\d$|\\d$)" //2018-07-27 18:31:18
    val regex4 = "[1-9]\\d{3}/(0[1-9]|1[0-2]|[1-9])/(0[1-9]|[1-2][0-9]|3[0-1]|[1-9])\\s+" +
      "(2[0-3]|[0-1]\\d|[0-9]):([0-5]\\d|[0-9]):([0-5]\\d$|\\d$)" //2018/07/27 18:31:18
*/
   val regex1 = "(19[7-9]\\d|20[0-2]\\d)-(0[1-9]|1[0-2]|[1-9])-(0[1-9]|[1-2][0-9]|3[0-1]|[1-9])\\s+" +
     "(2[0-3]|[0-1]\\d|\\d):([0-5]\\d|\\d):([0-5]\\d|\\d).\\d{1,3}" //2018-07-27 18:31:18.[f..]
    val regex2 = "(19[7-9]\\d|20[0-2]\\d)/(0[1-9]|1[0-2]|[1-9])/(0[1-9]|[1-2][0-9]|3[0-1]|[1-9])\\s+" +
      "(2[0-3]|[0-1]\\d|\\d):([0-5]\\d|\\d):([0-5]\\d|\\d).\\d{1,3}" //2018/07/27 18:31:18.[f..]
    val regex3 = "(19[7-9]\\d|20[0-2]\\d)-(0[1-9]|1[0-2]|[1-9])-(0[1-9]|[1-2][0-9]|3[0-1]|[1-9])\\s+" +
      "(2[0-3]|[0-1]\\d|[0-9]):([0-5]\\d|[0-9]):([0-5]\\d$|\\d$)" //2018-07-27 18:31:18
    val regex4 = "(19[7-9]\\d|20[0-2]\\d)/(0[1-9]|1[0-2]|[1-9])/(0[1-9]|[1-2][0-9]|3[0-1]|[1-9])\\s+" +
      "(2[0-3]|[0-1]\\d|[0-9]):([0-5]\\d|[0-9]):([0-5]\\d$|\\d$)" //2018/07/27 18:31:18

    var isTime: Boolean = true
    if (str.matches(regex1) || str.matches(regex2) || str.matches(regex3) || str.matches(regex4))
      isTime = true
    else
      isTime = false

    isTime
  }

  /**
    * 判断一个字符是否是合法的经度
    *
    * @param str 经度字符串
    * @return
    */
  def isLongitude(str: String): Boolean = {
    var result: Boolean = false
    //能转化为数值类型的正则表达式
    val patternStr = "^((-?\\d+.?\\d*)[Ee]?(-?\\d*))$"
    if (str.matches(patternStr)) {
      val longitude = str.toDouble
      result = math.abs(longitude) <= 180
    }
    result
  }

  def isLatitude(str: String): Boolean = {
    var result: Boolean = false
    //能转化为数值类型的正则表达式
    val patternStr = "^((-?\\d+.?\\d*)[Ee]?(-?\\d*))$"
    if (str.matches(patternStr)) {
      val latitude = str.toDouble
      result = math.abs(latitude) <= 90
    }
    result
  }

  //计算gps点的速度
  def speed(xlat: Double, xlon: Double, xtime: Timestamp, ylat: Double, ylon: Double, ytime: Timestamp): Double = {
    val ti = abs(durationSeconds(xtime, ytime))
    val di = distanceOf2point(xlat, xlon, ylat, ylon)
    if (ti == 0) {
      0
    } else {
      di / ti
    }
  }

  //计算gps点的加速度
  def accelSpeed(xtime: Timestamp, ytime: Timestamp, v1: Double, v2: Double): Double = {
    val ti = abs(durationSeconds(xtime, ytime))
    val si = v2 - v1
    if (ti == 0) {
      0
    } else {
      si / ti
    }
  }

  /**
    * 根据两个位置的经纬度，来计算两地的距离（单位: 米）
    * 这种方法只适合较短距离，一般500km以内较为准确.
    * note：轨迹不能穿越经度正负的180度分界线！
    */
  def distanceSimple(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    //math.sqrt((lon1-lon2)*(lon1-lon2) + (lat1-lat2)*(lat1-lat2))/180*Math.PI*6371
    // 因为：1.0/180*Math.PI*6371=111.19492664455873, 用计算结果稍作简化
    math.sqrt((lon1 - lon2) * (lon1 - lon2) + (lat1 - lat2) * (lat1 - lat2)) * 111194.93
  }

  def samePlace(xlat: Double, xlon: Double, ylat: Double, ylon: Double): Boolean = {
    (xlat == ylat) && (xlon == ylon)
  }

}
