package hudongbaike

import java.util
import java.util.regex.Pattern

import org.apache.hadoop.fs.{FileSystem, Path, Trash}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ExtractUserFromHtml {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName(getClass.getSimpleName))
    if (args.length < 2) {
      println("Usage: INPUT+  OUTPUT")
      System.exit(0)
    }


    var allRdd: RDD[String] = null

    var idx = 0
    // val prefix = args(0)

    // for 循环
    for( idx <- 0 until args.length - 1){
      var newIdsRdd =  util.ScalaUtil.getRddFromHdfs(sc, args(idx))
      if (allRdd == null){
        allRdd = newIdsRdd
      } else {
        allRdd = allRdd.union(newIdsRdd)
      }
    }
    val output = args(args.length - 1)
    if (FileSystem.get(sc.hadoopConfiguration).exists(new Path(output))) new Trash(sc.hadoopConfiguration).moveToTrash(new Path(output))

    var host:String="http://i.baike.com/"
    var patterns: util.List[Pattern]=new util.LinkedList[Pattern]()
    patterns.add(Pattern.compile("useriden=(.*)&prd="))


    ExtractUserFromHtml.extractUrlFromHtml(allRdd,host,patterns,output)

  }

  def extractUrlFromHtml(allRdd: RDD[String],host:String,patterns:  util.List[Pattern],output:String): Unit = {

    val retRdd =  allRdd
      .repartition(100)

      .flatMap((html)=>{

        var wordList = utilby.ExtractUrlFromHtml.extractUrlFromHtml(html.toString(),host,patterns)
        wordList.toArray()
      }
      )
      .map("http://i.baike.com/profile.do?action=more&useriden="+_+"&t=0.7325755269688712")
      .distinct()
    retRdd.coalesce(1).saveAsTextFile(output)
  }

}
