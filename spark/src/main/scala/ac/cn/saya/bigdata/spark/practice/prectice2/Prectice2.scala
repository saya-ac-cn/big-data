package ac.cn.saya.bigdata.spark.practice.prectice2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Title: Prectice2
  * @ProjectName big-data
  * @Description: TODO
  * @Author Pandora
  * @Date: 2020/9/4 11:29
  * @Description:
  */
object Prectice2 {
  def main(args: Array[String]): Unit = {
    //1.初始化配置信息及sc
    val conf = new SparkConf().setAppName("Prectice2").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //2.创建一个RDD
    val source:RDD[String] = sc.parallelize(Array("class:hadoop", "class:spark", "class:hive", "class:hbase"))

    //3.创建一个Search对象,其中Search必须序列化
    val search = new Search()
    val result1 = search.getMatch1(source)
    result1.take(100).foreach(println)
    println("------------------")
    val result2 = search.getMatch2(source)
    result2.take(100).foreach(println)
  }
}
