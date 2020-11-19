package basic
import java.io.File
import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object IOMemory {
  /** 工程文件的根目录 */
  val projectRoot: String = {
    val dir = new File("")
    val path = dir.getAbsolutePath.replaceAll("\\\\", "\\/")
    println(s"project的根目录为: ${path.split("/").slice(0, 2).mkString("", "/", "/")}")
    path.split("/").slice(0, 2).mkString("", "/", "/")
  }

  val zzjzParam = "<#zzjzParam#>"
  val rddTableName = "<#rddtablename#>"

  /** 用于存放任意类型的HashMap */
  val outputrdd4Any: java.util.Map[java.lang.String, Any] = new util.HashMap[java.lang.String, Any]()

  /** 用于存放引用类型的HashMap */
  val outputrdd: java.util.Map[java.lang.String, Object] = new util.HashMap[java.lang.String, Object]()

}
/**
 * Created by pc on 2018/3/31.
 */
class Z {
  private val rddList = new util.LinkedHashMap[String, AnyRef]

  def rdd(rddName: String): Any = rddList.get(rddName)

  def uput(key: String, value: Any): Unit = {
    IOMemory.outputrdd4Any.put(key, value)
  }

  def uget(key: String): Any = {
    if (IOMemory.outputrdd4Any containsKey key)
      IOMemory.outputrdd4Any.get(key)
    else if (IOMemory.outputrdd containsKey key)
      IOMemory.outputrdd.get(key)
    else
      null
  }

  def get(key: String): Any = {
    if (IOMemory.outputrdd4Any containsKey key)
      IOMemory.outputrdd4Any.get(key)
    else if (IOMemory.outputrdd containsKey key)
      IOMemory.outputrdd.get(key)
    else
      null
  }

  def put(key: String, value: Any): Unit = {
    IOMemory.outputrdd4Any.put(key, value)
  }
}

abstract class BaseMain {
  System.setProperty("hadoop.home.dir", "D:\\tools\\hadoop-common-2.2.0-bin-master")
  lazy val cfg: SparkConf =new SparkConf().setAppName("zzjz-deepinsight-algorithm").setMaster("local[*]").set("spark.cores.max","8")
  lazy val spark: SparkSession =SparkSession.builder().config(cfg).config("org.spark.sql.warehouse.dir","file:///")
          .getOrCreate()
  lazy val sc: SparkContext =spark.sparkContext

  //设置一些额外的参数
  sc.setLogLevel("WARN")
  sc.setCheckpointDir("D:\\checkpointfiles\\")
  lazy val sqlc: SQLContext =spark.sqlContext
  lazy val hqlc: SparkSession =spark
  lazy  val ssc : StreamingContext = new StreamingContext(cfg, Seconds(1))
  lazy val outputrdd: java.util.Map[java.lang.String,java.lang.Object]= new util.HashMap[java.lang.String,java.lang.Object]()
  lazy val z:Z = new Z()

  def run():Unit

  def main(args: Array[String]): Unit = {
    run()
    spark.stop()
  }

  def outputRDD(str:String,obj:Object):java.util.Map[java.lang.String,java.lang.Object]={
    new util.HashMap[java.lang.String,java.lang.Object]()
  }

  def inputRDD(str:String):Object={
    val rddList = new util.LinkedHashMap[String,Object]()
    rddList.get(str)
  }

}