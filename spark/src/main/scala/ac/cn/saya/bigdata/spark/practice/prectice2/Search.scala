package ac.cn.saya.bigdata.spark.practice.prectice2

import org.apache.spark.rdd.RDD

/**
  * @Title: Search
  * @ProjectName big-data
  * @Description: TODO
  * @Author Pandora
  * @Date: 2020/9/3 11:33
  * @Description:
  */
class Search() extends Serializable{

  // 过滤出包含字符串的数据
  def isMatch(value:String):Boolean={
    value.contains("class")
  }

  def getMatch1(rdd:RDD[String]):RDD[String]={
    rdd.filter(isMatch)
  }

  def getMatch2(rdd:RDD[String]):RDD[String]={
    rdd.filter(item => item.contains("class"))
  }

}
