package utilby

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object ScalaUtil {

  def getRddFromHdfs(sc: SparkContext, input: String): RDD[String] = {

    val compactData = sc.newAPIHadoopFile(input, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], sc.hadoopConfiguration)
      .map(l => l._2.toString) //每行信息为url，html 仅输入html进入rdd
    compactData
  }




}
