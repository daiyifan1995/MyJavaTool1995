package hudongbaike

import java.text.SimpleDateFormat

import com.google.gson.{Gson, JsonObject}
import hudongbaike.ExtractUserFromHtml.getClass
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SortUserBySpeed {
  val GSON = new Gson()
  private val df = new SimpleDateFormat("yyyy-MM-dd HH:mm")

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName(getClass.getSimpleName))
    val output=args(args.length-1)

    var allRdd: RDD[String] = null

    var idx = 0
    // val prefix = args(0)

    // for 循环
    for (idx <- 0 until args.length - 1) {
      var newIdsRdd = utilby.ScalaUtil.getRddFromHdfs(sc, args(idx))
      if (allRdd == null) {
        allRdd = newIdsRdd
      } else {
        allRdd = allRdd.union(newIdsRdd).distinct()
      }
    }

    var rddTuple = allRdd.map { line =>
      var jsonobj = GSON.fromJson(line, classOf[JsonObject])
      if (jsonobj.has("UserId") && jsonobj.has("RecordCount") &&
        jsonobj.has("LastUpdateTime") && jsonobj.has("UpdateSpeed") &&
        jsonobj.has("DownTime")) {
        var userId = jsonobj.get("UserId").getAsString
        var recordCount = jsonobj.get("RecordCount").getAsInt
        var lastUpdateTime =df.parse( jsonobj.get("LastUpdateTime").getAsString)
        var downTime = df.parse(jsonobj.get("DownTime").getAsString)
        var updateSpeed = jsonobj.get("UpdateSpeed").getAsLong

        (userId, recordCount, lastUpdateTime, downTime, updateSpeed)

      }
      else {
        (0, 0, 0, 0, 0)
      }

    }.filter(_._1 != 0)

    var sortRdd=rddTuple.sortBy(x=>x._5,false).coalesce(1).saveAsTextFile(output)


  }
}
