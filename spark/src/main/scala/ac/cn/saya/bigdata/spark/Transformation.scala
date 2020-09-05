package ac.cn.saya.bigdata.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/**
  * @Title: Transformation
  * @ProjectName big-data
  * @Description: TODO
  * @Author liunengkai
  * @Date: 2020-09-04 22:30
  * @Description:
  */
object Transformation {

  val conf = new SparkConf().setAppName("Transformation").setMaster("local[*]")
  val sc = new SparkContext(conf)

  def map():Unit={
    // 创建
    var source = sc.parallelize(1 to 10)
    // 打印 ,慎用collect，会引起驱动程序在内存外运行，因为collect()获取整个RDD到一台单机上，建议用take
    source.take(100).foreach(println)
    // 针对RDD里面的每一个元素*2
    val newRdd = source.map(item => item*2)//亦可(_*2)
    newRdd.take(100).foreach(println)
  }

  def mapPartitions():Unit={
    val source = sc.makeRDD(1 to 10)
    val newRdd = source.mapPartitions( item => item.map(_*2))
    newRdd.take(100).foreach(println)
  }

  def mapPartitionsWithIndex():Unit={
    val source = sc.makeRDD(1 to 10)
    val newRdd = source.mapPartitionsWithIndex((index,item) => (item.map((index,_))))
    newRdd.take(100).foreach(println)
  }

  def flatMap():Unit={
    val source = sc.parallelize(Array(Array("a;s;d","f;j;k","y;u;j;k"),Array("s;d;f","y;u;k","t;y;j"),Array("o;p","q;w","z;x")))
    val newRdd = source.flatMap(item => item.map(_.split(";")))
    newRdd.take(100).foreach(println)
  }

  def groupBy():Unit={
    val source = sc.parallelize(1 to 10)
    val newRdd = source.groupBy(item => item % 2)
    newRdd.take(100).foreach(println)
  }

  def filter():Unit={
    val source = sc.parallelize(1 to 100)
    val newRdd = source.filter(item => if(item%5==0)true else false)
    newRdd.take(100).foreach(println)
  }

  def sample():Unit={
    val source = sc.parallelize(1 to 10)
    val sample1 = source.sample(true,0.4,2000)
    sample1.take(100).foreach(println)
  }

  def distinct():Unit={
    val source = sc.parallelize(Array(1,24,67,6,345,234,67,23,67))
    val newRdd = source.distinct()
    newRdd.take(100).foreach(println)
  }

  def coalesce():Unit={
    val source = sc.parallelize(1 to 10 ,4)
    println("原来的分区："+source.partitions.size)
    // 可以选择是否进行shuffle过程
    val coalesceRDD = source.coalesce(3,true)
    println("新的分区："+coalesceRDD.partitions.size)
  }

  def repartition():Unit={
    val source = sc.parallelize(1 to 10 ,4)
    println("原来的分区："+source.partitions.size)
    val coalesceRDD = source.repartition(2)
    println("新的分区："+coalesceRDD.partitions.size)
  }

  def sortBy():Unit={
    val source = sc.parallelize(Array(1,24,67,6,345,234,67,23,67))
    // - 是降序
    val newRdd = source.sortBy(item => -item)
    newRdd.take(100).foreach(println)
  }

  def union():Unit={
    val source1 = sc.parallelize(1 to 5)
    val source2 = sc.parallelize(6 to 10)
    var source = source1.union(source2)
    source.take(100).foreach(println)
  }

  def subtract():Unit={
    val source1 = sc.parallelize(1 to 5)
    val source2 = sc.parallelize(3 to 8)
    // 从source1中排除source2
    var source3 = source1.subtract(source2)
    source3.take(100).foreach(println)
    println("------------------------")
    // 从source2中排除source1
    var source4 = source2.subtract(source1)
    source4.take(100).foreach(println)
  }

  def intersection():Unit={
    val source1 = sc.parallelize(1 to 5)
    val source2 = sc.parallelize(3 to 8)
    var source = source1.intersection(source2)
    source.take(100).foreach(println)
  }

  /**
    * 避免使用
    */
  def cartesian():Unit={
    val source1 = sc.parallelize(Array(1,2))
    val source2 = sc.parallelize(Array(3,4,5))
    var source = source1.cartesian(source2)
    source.take(100).foreach(println)
  }

  /**
    * 将两个RDD组合成Key/Value形式的RDD,这里默认两个RDD的partition数量以及元素数量都相同，否则会抛出异常
    */
  def zip():Unit={
    val source1 = sc.parallelize(Array(1,2,3),2)
    val source2 = sc.parallelize(Array("a","b","c"),2)
    var source3 = source1.zip(source2)
    source3.take(100).foreach(println)
    println("----------------")
    var source4 = source2.zip(source1)
    source4.take(100).foreach(println)
  }

  def main(args: Array[String]): Unit = {
    map()
  }

}
