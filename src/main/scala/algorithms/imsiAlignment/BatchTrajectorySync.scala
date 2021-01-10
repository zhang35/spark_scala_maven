package algorithms.imsiAlignment

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.google.gson.{GsonBuilder, JsonObject, JsonParser}
import org.apache.http.client.methods.HttpPut
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/**
 * @project zzjz-spk2.1.3hw
 * @autor 杨书轩 on 2020/11/30
 */
case class BatchTrajectorySync(sparkSession:SparkSession){
  import sparkSession.implicits._

  implicit object TimestampOrdering extends Ordering[Timestamp] {
    override def compare(p1: Timestamp, p2: Timestamp): Int = {
      p1.compareTo(p2)
    }
  }
  def run(jsonparamStr:String,inputRDD:String => Object):DataFrame={
    val jsonParam = (new JsonParser()).parse(jsonparamStr).getAsJsonObject

    def extractField(jsonObject: JsonObject,fieldNames:String*):String = {
      fieldNames.toList match {
        case xh::Nil =>
          jsonObject.get(xh).getAsString
        case xh::xs =>
          extractField(jsonObject.get(xh).getAsJsonObject,xs:_*)

      }
    }

    val tableName = extractField(jsonParam,"tableName")
    val IDstr = extractField(jsonParam,"ID")
    val time = extractField(jsonParam,"time")
    val jingdu = extractField(jsonParam,"longitude")
    val weidu = extractField(jsonParam,"latitude")
    val url = extractField(jsonParam,"url")
    val batchsize = extractField(jsonParam,"batchsize").toInt



    val inputDf = inputRDD(tableName).asInstanceOf[DataFrame]
    val inputDs = inputDf.select(
      col(IDstr) as "object_id",
      col(time) as "lastmodified_time",
      col(jingdu) as "lng", col(weidu) as "lat").distinct().na.drop().as[LastappearedModel]//.dropDuplicates("timeStamp")
    syncTrajectory(inputDs,url,batchsize)
    inputDs.toDF()


  }
  def syncTrajectory(lastappearedModels:Dataset[LastappearedModel],url:String="http://127.0.0.1:5000/batch/update/lastappeared",batchsize:Int=10): Unit ={
    lastappearedModels.groupByKey(l => l.object_id -> new SimpleDateFormat("yyyy-MM-dd").format(l.lastmodified_time)
      ).mapGroups((k,s) => s.toList.sortBy(_.lastmodified_time)).foreach{v =>


      v.grouped(batchsize).foreach{u =>
        val t = u.toList
        val w:BatchLastappearedModel = BatchLastappearedModel(t.head.object_id,t)

        val client = HttpClients.createDefault()
        val gsonBuildr = new GsonBuilder();
        gsonBuildr.registerTypeAdapter(classOf[BatchLastappearedModel], new BatchLastappearedModelSerializer());
        val httpPut = new HttpPut(s"${url}/${w.object_id}")
        val  json = gsonBuildr.create().toJson(w)
        val entity = new StringEntity(json);
        httpPut.setEntity(entity);
        httpPut.setHeader("Accept", "application/json");
        httpPut.setHeader("Content-type", "application/json");


        val response =  client.execute(httpPut);
        println(response.getStatusLine().getStatusCode())
        client.close();
      }


    }

  }
}
