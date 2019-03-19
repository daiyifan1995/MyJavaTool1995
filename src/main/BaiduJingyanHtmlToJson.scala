
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path, Trash}
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.spark.SparkContext
import org.jsoup.Jsoup
import com.google.gson.JsonObject

import scala.collection.mutable

import com.google.gson.Gson;

import com.xiaomi.kg.dataflow.common.StringUtil;


object BaiduJingyanHtmlToJson {
  val GSON = new Gson()

  def main(args: Array[String]): Unit = {
    val conf = SparkHelper.createSparkConf(getClass.getSimpleName).set("spark.speculation", "false")
    val sc = new SparkContext(conf)
    
	
    val argMap = ArgumentParser.parse(args)
    val input = argMap("input")
    var output = argMap("output")
    if (!output.endsWith("/")) output += "/"
    if (FileSystem.get(sc.hadoopConfiguration).exists(new Path(output))) new Trash(sc.hadoopConfiguration).moveToTrash(new Path(output))


    val originRdd = sc.newAPIHadoopFile(input, classOf[KeyValueTextInputFormat], classOf[Text], classOf[Text], sc.hadoopConfiguration)
      .map(l => (l._1.toString, l._2.toString))
      .repartition(100).cache()


    var articleRdd =   originRdd.filter(_._1.contains("/article/"))
      .map { case (id, html) =>
        var url = StringUtil.getUniqBaiduJingyanUrl(id.toString)
        val jsonObj = new JsonObject()
        var ret = BaiduJingyanHtmlToJsonParser.extractArticle(html.toString, url, jsonObj);
        var fetchTime = ""
        if (ret){
          fetchTime = jsonObj.get("fetchTime").getAsString()
        }
		    (ret, url, jsonObj.toString, fetchTime)
      }
     
    articleRdd.filter(_._1)
    .map(e => (e._2, (e._3, e._4)))
    .reduceByKey {
	  case (one, two) =>
	    if (one._2.compareTo(two._2) > 0) {
	        one
	    } else {
	        two
      }
	  }
	  .map(_._2._1).coalesce(50).saveAsTextFile("%s%s".format(output, "QAPair"), classOf[GzipCodec])

    var userRdd =   originRdd.filter(_._1.contains("/user/npublic"))
      .map { case (id, html) =>
        var url = StringUtil.getUniqBaiduJingyanUrl(id.toString)
        val jsonObj = new JsonObject()
        var uid = BaiduJingyanHtmlToJsonParser.getUidFromUrl(url)
        var ret = BaiduJingyanHtmlToJsonParser.extractUser(html.toString, url, jsonObj);
        var fetchTime = ""
        if (ret){
          fetchTime = jsonObj.get("fetchTime").getAsString()
        }
        (ret, uid, jsonObj.toString, fetchTime)
      }
     
    userRdd.filter(_._1)
    .map(e => (e._2, (e._3, e._4)))
    .reduceByKey {
    case (one, two) =>
      if (one._2.compareTo(two._2) > 0) {
          one
      } else {
          two
      }
    }
    .map(_._2._1).coalesce(1).saveAsTextFile("%s%s".format(output, "user"), classOf[GzipCodec])
	  
    // originRdd.filter(_._1).map(_._2).distinct().saveAsTextFile("%s%s".format(output, "badUrl"), classOf[GzipCodec])

    originRdd.unpersist()
    
  }
}
