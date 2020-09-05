package ac.cn.saya.bigdata.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
/**
  * @Title: DataFrame
  * @ProjectName big-data
  * @Description: TODO
  * @Author Pandora
  * @Date: 2020/9/4 13:53
  * @Description:
  * import spark.implicits._ 报红，无法导入
  * 解决方法：给SparkSession.builder一个对应的变量值，这个变量值是spark。
  * 这里的spark不是某个包下面的东西，而是我们SparkSession.builder()对应的变量值，下面是正确的写法
  *
  * def main(args: Array[String]): Unit = {
  *   //Create SparkConf() And Set AppName
  *   val spark=  SparkSession.builder()
  *   .appName("Spark Sql basic example")
  *   .config("spark.some.config.option", "some-value")
  *   .getOrCreate()
  *   //import implicit DF,DS
  *   import spark.implicits._
  * }
  * 来源：https://www.cnblogs.com/fanta2000/p/12252497.html
  */
object DataFrame {

  val conf = new SparkConf().setAppName("DataFrame").setMaster("local[*]")
  val sc = new SparkContext(conf)
  private val spark: SparkSession = SparkSession.builder().appName("DataFrame").master("local").getOrCreate()
  // 下面一行语句需要放在获取SparkSession对象的语句之后
  import spark.implicits._
  // 下面语句的定义 需要放在方法的作用域之外（即java的成员变量位置）
  case class People(id:Int,name:String)
  //定义字段名和类型
  case class PeopleRow(id:Int,name:String)extends Serializable



  def experience():Unit={
    // 通过json 创建一个DataFrame
    val dataFrame = spark.read.json("file:///F:\\Saya\\project\\big-data\\spark\\src\\main\\resources\\sample.json")
    // 打印所有的数据
    dataFrame.show()
    // 过滤id大于1的数据
    dataFrame.filter(item => item.getAs[Long]("id")>1).show()
    // 对DataFrame创建一个临时表
    // 临时表是Session范围内的，Session退出后，表就失效了。如果想应用范围内有效，可以使用全局表
    dataFrame.createOrReplaceTempView("user")
    // 通过SQL语句实现查询
    spark.sql("select user from user where id > 1").show()
    spark.stop()
  }

  /**
    * 手动确定转换
    */
  def rddToDateFrame1():Unit={
    val rdd = sc.parallelize(Array((1,"saya"),(2,"pandora"),(3,"shmily")))
    rdd.map(item => (item._1,item._2)).toDF("id","name").show()
  }

  /**
    * 通过反射
    */
  def rddToDateFrame2():Unit={
      val rdd = sc.parallelize(Array((1,"saya"),(2,"pandora"),(3,"shmily")))
      rdd.map(item => {
        People(item._1,item._2)
      }).toDF.show()
  }

  /**
    * 通过编程
    */
  def rddToDateFrame3():Unit={
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.Row
    val peopleStruct:StructType  = StructType(StructField("id",IntegerType)::StructField("name",StringType)::Nil)
    val rdd = sc.parallelize(Array((1,"saya"),(2,"pandora"),(3,"shmily")))
    val data = rdd.map(item => {
      Row(item._1, item._2)
    })
    val df = spark.createDataFrame(data,peopleStruct)
    df.show()
  }

  def dataFrameToRdd():Unit={
    val rdd = sc.parallelize(Array((1,"saya"),(2,"pandora"),(3,"shmily")))
    val dataFrame = rdd.map(item => (item._1,item._2)).toDF("id","name")
    dataFrame.rdd.take(10).foreach(println)
  }

  def dataFrameToDataSet():Unit={
    val rdd = sc.parallelize(Array((1,"saya"),(2,"pandora"),(3,"shmily")))
    val df = rdd.map(item => (item._1,item._2)).toDF("id","name")
    df.as[PeopleRow].show()
  }

  def main(args: Array[String]): Unit = {
    dataFrameToDataSet()
  }

}
