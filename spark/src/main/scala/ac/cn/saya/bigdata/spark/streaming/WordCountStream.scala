package ac.cn.saya.bigdata.spark.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Title: WordCountStream
  * @ProjectName big-data
  * @Description: TODO
  * @Author Pandora
  * @Date: 2020/9/4 16:23
  * @Description: 先启动 nc -l -p 8080，再执行本程序
  */
object WordCountStream {

  // local[*]是必须的 local 不行
  val conf = new SparkConf().setAppName("WordCountStream").setMaster("local[*]")

  def main(args: Array[String]): Unit = {

    // 初始化SparkStreamingContext
    val streamingContext = new StreamingContext(conf,Seconds(5))

    // 通过监听端口创建DStream，读进来的数据为一行行
    val lineStreams = streamingContext.socketTextStream("127.0.0.1", 8080)

    // 将每一行数据做切分，形成一个个单词,生成一元组（单词，1）
    val rdd =  lineStreams.flatMap(_.split(" ")).map((_,1))

    // 对单词进行词频统计
    val result = rdd.reduceByKey(_+_)

    // 打印
    result.print()

    //启动SparkStreamingContext
    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
