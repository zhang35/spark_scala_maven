package algorithms.imsiAlignment

import java.lang.reflect.Type
import java.text.SimpleDateFormat

import com.google.gson.{JsonElement, JsonObject, JsonSerializationContext, JsonSerializer}

/**
 * @project zzjz-spk2.1.3hw
 * @autor 杨书轩 on 2020/11/19
 */
class DifferentNameSerializer extends JsonSerializer[LastappearedModel] {
  def serialize(src: LastappearedModel, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
    val jObject = new JsonObject
    jObject.addProperty("object_id", src.object_id)
    jObject.addProperty("lastmodified_time", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(src.lastmodified_time)) //new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date)
    jObject.addProperty("long", src.lng)
    jObject.addProperty("lat", src.lat)
    jObject
  }
}