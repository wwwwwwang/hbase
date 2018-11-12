import net.minidev.json.{JSONArray, JSONObject, JSONValue}

import scala.collection.JavaConversions.mutableMapAsJavaMap
import scala.collection.mutable

/**
  * Created by Madhouse on 2018/4/10.
  */
object Test {
  def map2Json(map: mutable.Map[String, Any]): String = {
    val jsonString = JSONObject.toJSONString(map)
    jsonString
  }

  def string2Array(str:String): Array[String] ={
    val subStr = str.substring(str.indexOf("[")+1, str.indexOf("]")).replaceAll("\"","")
    subStr.split(",")
  }

  def getDid(str:String): String ={
    str.substring(str.indexOf(":")+1, str.indexOf(",")).replaceAll("\"","")
  }

  def main(args: Array[String]): Unit = {
    /*val a = mutable.Map[String, Any]()
    val value = mutable.ArrayBuffer[String]()
    a += ("did" -> "1234567890")
    value += "a"
    value += "b"
    value += "c"
    value += "d"
    a += ("tags" -> value.toArray)
    val res = map2Json(a)
    println(s"######res=$res")

    val json = JSONValue.parse(res).asInstanceOf[JSONObject]
    val did = json.getOrDefault("did","........")
    val tags = json.get("tags")
    println(s"#####did = $did, tags=$tags")*/

    val str = "{\"did\":\"0f939ac87c40a5a55716901f099ca61b\",\"tags\":[\"kd_017001003003\",\"kd_017004001004\",\"kd_017004002003\"]}"
    val array = string2Array(str)
    for(a <- array){
      println(a)
    }
    val did = getDid(str)
    println(s"#####did = $did")
  }
}
