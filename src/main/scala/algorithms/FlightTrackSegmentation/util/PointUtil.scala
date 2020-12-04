/**
 * Created by zhangjiaqi on 2018/07/17.
 */

package algorithms.FlightTrackSegmentation.util

import java.lang.Math._
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.commons.math3.geometry.euclidean.twod.Vector2D


object PointUtil extends Serializable {
  /**
   * 角度转弧度
   *
   * @param degree 角度
   * @return
   */
  def convertDegreesToRadians(degree: Double): Double = {
    (degree * PI) / 180
  }
    //返回地球两点距离，单位：m
  def distanceOf2point(p1: PointWithAttributes, p2: PointWithAttributes): Double = {
    val EARTH_RADIUS = 6378137
    val radLat1 = convertDegreesToRadians(p1.y)
    val radLng1 = convertDegreesToRadians(p1.x)
    val radLat2 = convertDegreesToRadians(p2.y)
    val radLng2 = convertDegreesToRadians(p2.x)
    val a = radLat1 - radLat2
    val b = radLng1 - radLng2
    val distance = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2)
            + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2))) * EARTH_RADIUS
    distance
  }

  def angle(p1: PointWithAttributes, p2: PointWithAttributes, p3: PointWithAttributes): Double = {
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
	* @param p1
     * @param p2
     * @param p3
	* @return 得到p1-p2-p3的转角，结果范围[-180,180]，逆时针（左转）为正，顺时针（右转）为负
	**/
  def degree(p1: PointWithAttributes, p2: PointWithAttributes, p3: PointWithAttributes): Double = {
    var degree: Double = 0.0
    if (!p2.samePosition(p1) && !p3.samePosition(p2)) {
      //向量p1p2
      val vector1 = new Vector2D(p2.x - p1.x, p2.y - p1.y)
      //向量p2p3
      val vector2 = new Vector2D(p3.x - p2.x, p3.y - p2.y)

      val angle = Vector2D.angle(vector1, vector2)

      degree = angle / PI * 180.0
      if (crossProduct(p1, p2, p3) < 0)
        degree = -degree
    }
    degree
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
  def crossProduct(p1: PointWithAttributes, p2: PointWithAttributes, p3: PointWithAttributes): Double = {
    val crossProduct = (p2.x - p1.x) * (p3.y - p2.y) - (p2.y - p1.y) * (p3.x - p2.x)
    crossProduct
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
