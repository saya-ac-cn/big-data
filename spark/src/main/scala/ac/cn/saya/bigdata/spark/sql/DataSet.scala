package ac.cn.saya.bigdata.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * @Title: DataSet
  * @ProjectName big-data
  * @Description: TODO
  * @Author Pandora
  * @Date: 2020/9/4 15:10
  * @Description:
  */
object DataSet {

  val conf = new SparkConf().setAppName("DataSet").setMaster("local[*]")
  val sc = new SparkContext(conf)
  private val spark: SparkSession = SparkSession.builder().appName("DataSet").master("local").getOrCreate()
  // 下面一行语句需要放在获取SparkSession对象的语句之后
  import spark.implicits._
  // 下面语句的定义 需要放在方法的作用域之外（即java的成员变量位置）
  case class People(id:Int,name:String)

  def create():Unit={
    val ds = Seq(People(1,"saya")).toDS()
    ds.show()
  }

  def rddToDataSet():Unit={
    val rdd = sc.parallelize(Array((1,"saya"),(2,"pandora"),(3,"shmily")))
    rdd.map(line => People(line._1,line._2)).toDS().show()
  }

  def dataSetToRdd():Unit={
    val rdd = sc.parallelize(Array((1,"saya"),(2,"pandora"),(3,"shmily")))
    val ds = rdd.map(line => People(line._1,line._2)).toDS()
    ds.rdd.take(100).foreach(println)
  }

  /**
    * 需导入import spark.implicits._隐式转换
    */
  def dataSetToDataFrame():Unit={
    val rdd = sc.parallelize(Array((1,"saya"),(2,"pandora"),(3,"shmily")))
    val ds = rdd.map(line => People(line._1,line._2)).toDS()
    ds.toDF.show()
  }

  def main(args: Array[String]): Unit = {
    dataSetToDataFrame()
  }

}
