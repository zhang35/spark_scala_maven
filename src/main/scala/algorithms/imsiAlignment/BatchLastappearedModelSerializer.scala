package algorithms.imsiAlignment

import java.lang.reflect.Type

import com.google.gson._

/**
 * @project zzjz-spk2.1.3hw
 * @autor 杨书轩 on 2020/11/30
 */
class BatchLastappearedModelSerializer extends JsonSerializer[BatchLastappearedModel] {
  def serialize(src: BatchLastappearedModel, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
    val gsonBuildr = new GsonBuilder();
    gsonBuildr.registerTypeAdapter(classOf[LastappearedModel], new DifferentNameSerializer());

    val jArray = new JsonArray
    for {
      l <- src.gps_points
    }{
      jArray.add(gsonBuildr.create().toJsonTree(l))
    }
    val jObject = new JsonObject

    jObject.addProperty("object_id", src.object_id)
    jObject.add("gps_points", jArray)

    jObject
  }
}