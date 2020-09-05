package ac.cn.saya.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Title: CreateRDD
  * @ProjectName big-data
  * @Description: TODO
  * @Author Pandora
  * @Date: 2020/9/2 14:39
  * @Description: RDD的创建
  */
object CreateRDD {

  val conf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
  val sc = new SparkContext(conf)

  /**
    * 从集合中创建
    */
  def byList(): Unit ={
    val rdd1 = sc.parallelize(Array(1,2,3,4,5,6,7,8))
    val rdd2 = sc.makeRDD(Array(1,2,3,4,5,6,7,8))
  }

  /**
    * 通过文件系统创建
    */
  def byFile():Unit = {
    // 通过本地文件
    val localPath =  "file:///Users/liunengkai/project/java/big-data/spark/src/main/resources/wordcount.txt"
    val rdd1 = sc.textFile(localPath)
    val hdfsPath = "hdfs://127.0.0.1:9000/spark.txt"
    val rdd2 = sc.textFile(hdfsPath)
  }

  /**
    * RDD的转换
    */

  def main(args: Array[String]): Unit = {
    byList()
  }

}
