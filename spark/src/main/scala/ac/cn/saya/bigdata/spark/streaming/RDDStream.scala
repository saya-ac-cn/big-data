package ac.cn.saya.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * @Title: RDDStream
  * @ProjectName big-data
  * @Description: TODO
  * @Author Pandora
  * @Date: 2020/9/4 18:14
  * @Description:
  */
object RDDStream {

  val conf = new SparkConf().setAppName("RDDStream").setMaster("local[*]")

  def main(args: Array[String]): Unit = {
    // 初始化SparkStreamingContext
    val streamingContext = new StreamingContext(conf,Seconds(5))

    // 创建RDD队列
    val rddQueue = new mutable.Queue[RDD[Int]]()

    // 创建QueueInputStream
    val inputStream = streamingContext.queueStream(rddQueue,oneAtATime = false)

    // 处理队列中的RDD数据
    val mappedStream = inputStream.map((_,1))
    val reduceStream = mappedStream.reduceByKey(_+_)

    // 打印
    reduceStream.print()

    // 启动
    streamingContext.start()

    // 循环创建并向RDD队列中放入RDD
    for (i <- 1 to 10){
      rddQueue += streamingContext.sparkContext.makeRDD(1 to 300,10)
      Thread.sleep(2000)
    }

    streamingContext.awaitTermination()
  }

}
