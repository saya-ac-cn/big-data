package ac.cn.saya.bigdata.spark.practice

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Title: Practice1
  * @ProjectName big-data
  * @Description: TODO
  * @Author Pandora
  * @Date: 2020/9/2 18:19
  * @Description: 统计出每一个省份广告被点击次数的TOP3
  * 数据结构：时间戳，省份，城市，用户，广告，中间字段使用空格分割
  * 1516609143867 6 7 64 16
  * 1516609143869 9 4 75 18
  * 1516609143869 1 7 87 12
  */
object Practice1 {
  def main(args: Array[String]): Unit = {

    val inputFile =  "file:///Users/liunengkai/project/java/big-data/spark/src/main/resources/agent.log"
    val conf = new SparkConf().setAppName("Practice1").setMaster("local")
    val sc = new SparkContext(conf)
    // 读取文件
    val textFile = sc.textFile(inputFile)
    // 数据清洗，这一步的rdd变成了（（省份，广告），1）
    val ad = textFile.map(line => {
      val fields = line.split(" ")
      ((fields(1),fields(4)),1)
    })
    // 计算按（省份，广告）分组后计算各广告的被点击总数，这一步的rdd变成了，（（省，广告），总数）
    val provinceCount = ad.reduceByKey(_+_)
    // 将省作为key，广告被点击数作为value，这一步的rdd变成了，（省（广告，总数））
    val provinceMap = provinceCount.map(item => (item._1._1,(item._1._2,item._2)))
    // 将同一省份的广告，分成一组，这一步的rdd变成了，（省，Array（（广告，总数），广告，总数）））
    val group = provinceMap.groupByKey()
    // 取出前3
    val top3 = group.mapValues(item => {
      item.toList.sortWith((x, y) => x._2 > y._2).take(3)
    })
    top3.take(100000).foreach(println)
    sc.stop()
  }
}
