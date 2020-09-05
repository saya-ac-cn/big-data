package ac.cn.saya.bigdata.spark.sql

import java.sql.{Connection, DriverManager}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Title: MysqlRDD
  * @ProjectName big-data
  * @Description: TODO
  * @Author Pandora
  * @Date: 2020/9/4 11:54
  * @Description:
  */
object MysqlRDD {

  val conf = new SparkConf().setAppName("MysqlRDD").setMaster("local[*]")
  val sc = new SparkContext(conf)

  def createConnection(): Connection = {
    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://127.0.0.1:3306/reptile?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true"
    val username = "root"
    val password = "root"
    Class.forName(driver)
    DriverManager.getConnection(url, username, password)
  }

  def readMysqlToRDD():Unit={
    val rdd = new JdbcRDD(sc,createConnection,
      "select code5,province,city,county,town,village from stats_init_data2020 where code1 = '510000000000' and 1 = ? and 1 = ?",
      1,
      1,
      1,
      r => (r.getString("code5"),(r.getString("province"),r.getString("city"),r.getString("county"),r.getString("town"),r.getString("village")))
    )
    println(rdd.count())
    rdd.foreach(println)
  }

  def rddInsertMysql():Unit={
    val rdd = sc.parallelize(Array((1,"saya"),(2,"pandora"),(3,"shmily")))
    rdd.foreach(item => {
      val ps = createConnection.prepareStatement("insert into user values(?,?)")
      ps.setInt(1,item._1)
      ps.setString(2,item._2)
      ps.executeLargeUpdate()
    })
  }

  def main(args: Array[String]): Unit = {
    rddInsertMysql()
  }
}
