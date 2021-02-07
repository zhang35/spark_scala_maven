package com.simgps

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import java.io.{File, FileOutputStream}
import scala.collection.mutable.ArrayBuffer
import scala.math.{asin, atan2, cos, sin}

/**
  *  Author:shifeng
  *  Date:2021/2/3
  *  Release Notes:
  *
  */
/**
  *
  *
  */
object GeneratePoints {

  val rootPath: String = "D:\\TestData\\gps\\017"

  val R = 6372.8

  def generateDest(lonR: Double,
                   latR: Double,
                   distance: Double,
                   bearing: Double) = {

    val a = 6378137
    val b = 6356752.3142
    val f = 1 / 298.257223563

    val lon1 = lonR //乘一（*1）是为了确保经纬度的数据类型为number
    val lat1 = latR

    val s = distance;
    val alpha1 = bearing.toRadians;
    val sinAlpha1 = Math.sin(alpha1);
    val cosAlpha1 = Math.cos(alpha1);

    val tanU1 = (1 - f) * Math.tan(lat1.toRadians);
    val cosU1 = 1 / Math.sqrt((1 + tanU1 * tanU1))
    val sinU1 = tanU1 * cosU1;

    val sigma1 = Math.atan2(tanU1, cosAlpha1);
    val sinAlpha = cosU1 * sinAlpha1;
    val cosSqAlpha = 1 - sinAlpha * sinAlpha;
    val uSq = cosSqAlpha * (a * a - b * b) / (b * b);
    val A = 1 + uSq / 16384 * (4096 + uSq * (-768 + uSq * (320 - 175 * uSq)));
    val B = uSq / 1024 * (256 + uSq * (-128 + uSq * (74 - 47 * uSq)));

    var sigma = s / (b * A)
    var sinSigma: Double = Math.sin(sigma)
    var cosSigma: Double = Math.cos(sigma)
    var cos2SigmaM = Math.cos(2 * sigma1 + sigma);

    var sigmaP = 2 * Math.PI;

    while (Math.abs(sigma - sigmaP) > 1e-12) {
      cos2SigmaM = Math.cos(2 * sigma1 + sigma);
      sinSigma = Math.sin(sigma)
      cosSigma = Math.cos(sigma)
      val deltaSigma = B * sinSigma * (cos2SigmaM + B / 4 * (cosSigma * (-1 + 2 * cos2SigmaM * cos2SigmaM) -
        B / 6 * cos2SigmaM * (-3 + 4 * sinSigma * sinSigma) * (-3 + 4 * cos2SigmaM * cos2SigmaM)));
      sigmaP = sigma
      sigma = s / (b * A) + deltaSigma;
    }

    val tmp = sinU1 * sinSigma - cosU1 * cosSigma * cosAlpha1;
    val lat2 = Math.atan2(
      sinU1 * cosSigma + cosU1 * sinSigma * cosAlpha1,
      (1 - f) * Math.sqrt(sinAlpha * sinAlpha + tmp * tmp)
    );
    val lambda = Math.atan2(
      sinSigma * sinAlpha1,
      cosU1 * cosSigma - sinU1 * sinSigma * cosAlpha1
    );
    val C = f / 16 * cosSqAlpha * (4 + f * (4 - 3 * cosSqAlpha));
    val L = lambda - (1 - C) * f * sinAlpha *
      (sigma + C * sinSigma * (cos2SigmaM + C * cosSigma * (-1 + 2 * cos2SigmaM * cos2SigmaM)));
    // val revAz = Math.atan2(sinAlpha, -tmp);  // final bearing
    val lon_destina = lon1 * 1 + L.toDegrees;

    (
      if (lon_destina > 180) -180 + lon_destina - 180 else lon_destina,
      if (lat2.toDegrees > 90) -90 + lat2.toDegrees - 90 else lat2.toDegrees
    )

//    val sinLat = sin(latR) * cos(distance / R) +
//      cos(latR) * sin(distance / R) * cos(bearing.toRadians)
//    val lat = asin(sinLat)
//    val y = sin(bearing.toRadians) * sin(distance / R) * cos(latR)
//    val x = cos(distance / R) - sin(latR) * sinLat
//    val lon = lonR + atan2(y, x)
//    (lon, lat)
  }

  def generateFirst() = {
    val area = Array(110.398, 18.103, 115.759, 8.059)
    val lon = (115.759 - 110.398) * scala.util.Random.nextDouble() + 110.398
    val lat = (18.103 - 8.059) * scala.util.Random.nextDouble() + 8.059
    (lon, lat)
  }

  def generateKm(minKm: Double, maxKm: Double): Double = {
    scala.util.Random.nextDouble() * (maxKm - minKm) + minKm
  }

  def generateBear(minB: Double, maxB: Double): Double = {
    scala.util.Random.nextDouble() * (maxB - minB) + minB
  }

  def generateL(): Int = {
    val lset = (1000 to 1005).toArray
    val id = scala.util.Random.nextInt(lset.length)
    lset(id)
  }

  def generateFreq(): Int = {
    val fset = (500000 to 500010).toArray
    val id = scala.util.Random.nextInt(fset.length)
    fset(id)
  }

  def generateT() = {
    val pattern = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    val startT = pattern.parseMillis("2021-01-10 06:00:00")
    val endT = pattern.parseMillis("2021-01-11 23:59:59")
    val res = (startT to endT).toArray
    val idx = scala.util.Random.nextInt(res.length)
    val ms = res(idx)

    new DateTime(ms)
  }

  def main2(args: Array[String]): Unit = {

    val res = generateDest(116.2, 25.0, 1000, 56)
    println(res)

  }

  def main(args: Array[String]): Unit = {

    val numTrack: Int = 20

    val step = 2

    val file = new File(rootPath + "\\gps.data")
    val fileOs = new FileOutputStream(file)
    val header = "imsi,gpstime,lon,lat,tbid,freq,flag\r\n"
    fileOs.write(header.getBytes("UTF-8"))

    (0 until numTrack).foreach { id =>
      val uid = java.util.UUID.randomUUID().toString.split("-").head
      val startKm: Double = scala.util.Random.nextFloat() * 10000 + 10000
      val endKm: Double = scala.util.Random.nextFloat() * 10000 + 20000
      val startB = scala.util.Random.nextFloat() * 30 + 120
      val endB = scala.util.Random.nextFloat() * 30 + 270
      val numPoint = scala.util.Random.nextInt(100) + 200

      var initT = generateT()

      val lon_lat = generateFirst()
      var lon = lon_lat._1
      var lat = lon_lat._2

      // 随机生成L号数据

      val lrand = Array(1, 2, 3, 4)

      val lid = lrand(scala.util.Random.nextInt(lrand.length))

      // 生成轨迹个数
      var cnt = 0
      var initL = generateL()
      var initF = generateFreq()
      var isL: Boolean = false
      val buffer: ArrayBuffer[(Int, Int)] = new ArrayBuffer[(Int, Int)]()
      buffer.append((initL, initF))
      (0 until numPoint).foreach { index =>
        // 生成一条记录
        val record =
          s"$uid,${initT.toString("yyyy-MM-dd HH:mm:ss")},$lon,$lat,$initL,$initF,$isL\r\n"
        // 写入后进行重置
        isL = false
        val flag = scala.util.Random.nextFloat() <= 0.2
        if (flag && cnt >= 60) {
          var isNext = true
          while (isNext) {
            initL = generateL()
            initF = generateFreq()
            isNext = buffer.exists(p => p._1 == initL || p._2 == initF)
          }
          buffer.append((initL, initF))
          cnt = 0
        }

        lid match {
          case 1 => if (scala.util.Random.nextFloat() < 0.5) isL = true
          case 2 => if (scala.util.Random.nextFloat() < 0.65) isL = true
          case 3 => if (scala.util.Random.nextFloat() < 0.8) isL = true
          case 4 => if (scala.util.Random.nextFloat() < 0.9) isL = true
        }

        cnt = cnt + 1
        val dist = generateKm(startKm, endKm)
        val bear = generateBear(startB, endB)
        println(dist, bear)
        val lon_lat2 = generateDest(lon, lat, dist, bear)
        lon = lon_lat2._1
        lat = lon_lat2._2
        initT = initT.plusMinutes(step)
        fileOs.write(record.getBytes("UTF-8"))

      }
    }

    fileOs.close()

  }

}
