package ac.cn.saya.bigdata.spark.practice.practic3

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Title: Practice3
  * @ProjectName big-data
  * @Description: TODO
  * @Author Pandora
  * @Date: 2020/9/4 15:43
  * @Description:
  */
object Practice3 {

  val conf = new SparkConf().setAppName("DataFrame").setMaster("local[*]")
  val sc = new SparkContext(conf)
  private val spark: SparkSession = SparkSession.builder().appName("DataFrame").master("local").getOrCreate()
  // 下面一行语句需要放在获取SparkSession对象的语句之后
  import spark.implicits._

  case class tbStock(ordernumber: String, locationid: String, dateid: String) extends Serializable

  case class tbStockDetail(ordernumber: String, rownum: Int, itemid: String, number: Int, price: Double, amount: Double) extends Serializable

  case class tbDate(dateid: String, years: Int, theyear: Int, month: Int, day: Int, weekday: Int, week: Int, quarter: Int, period: Int, halfmonth: Int) extends Serializable

  def load_tbStock(): Dataset[tbStock] = {
    val rdd = spark.sparkContext.textFile("file:///Users/liunengkai/project/java/big-data/spark/src/main/resources/tbStock.txt")
    rdd.map(_.split(",")).map(line => tbStock(line(0), line(1), line(2))).toDS()
  }

  def load_tbStockDetail(): Dataset[tbStockDetail] = {
    val rdd = spark.sparkContext.textFile("file:///Users/liunengkai/project/java/big-data/spark/src/main/resources/tbStockDetail.txt")
    rdd.map(_.split(",")).map(line => tbStockDetail(line(0), line(1).trim.toInt, line(2), line(3).trim.toInt, line(4).trim.toDouble, line(5).trim.toDouble)).toDS()
  }

  def load_tbDate(): Dataset[tbDate] = {
    val rdd = spark.sparkContext.textFile("file:///Users/liunengkai/project/java/big-data/spark/src/main/resources/tbDate.txt")
    rdd.map(_.split(",")).map(line => tbDate(line(0), line(1).trim.toInt, line(2).trim.toInt, line(3).trim.toInt, line(4).trim.toInt, line(5).trim.toInt, line(6).trim.toInt, line(7).trim.toInt, line(8).trim.toInt, line(9).trim.toInt)).toDS()
  }

  def main(args: Array[String]): Unit = {
    // 注册表
    load_tbStock.createOrReplaceTempView("tbStock")
    load_tbStockDetail.createOrReplaceTempView("tbStockDetail")
    load_tbDate.createOrReplaceTempView("tbDate")
    // 统计所有订单中每年的销售单数、销售总额
    spark.sql("select c.theyear,count(distinct a.ordernumber) as orders,sum(b.amount) as amount from tbStock a left join tbStockDetail b on a.ordernumber = b.ordernumber left join tbDate c on a.dateid = c.dateid group by c.theyear order by c.theyear").show
  }

}
