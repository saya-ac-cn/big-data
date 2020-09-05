package ac.cn.saya.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Title: Action
  * @ProjectName big-data
  * @Description: TODO
  * @Author Pandora
  * @Date: 2020/9/3 09:00
  * @Description:行动操作
  */
object Action {

  val conf = new SparkConf().setAppName("Action").setMaster("local[*]")
  val sc = new SparkContext(conf)

  def reduce(): Unit = {
    val source1 = sc.parallelize(1 to 100)
    println("sum=" + source1.reduce(_ + _))
    val source2 = sc.parallelize(Array(("a",1),("b",3),("c",3),("d",5)),2)
    val result = source2.reduce((x,y) => (x._1+";"+y._1,x._2+y._2))
    println(result)
  }

  /**
    * 慎用
    * 在驱动程序中，以数组的形式返回数据集的所有元素
    */
  def collect():Unit={
    val source = sc.parallelize(1 to 100,2)
    // 将结果收集到Driver端
    val array = source.collect()
    println("count="+source.count())
    println("第一个元素："+source.first())
    array.take(100).foreach(println)
  }

  def takeOrdered():Unit={
    val source = sc.parallelize(Array(1,986,234,89,2))
    val result = source.takeOrdered(3)
    result.foreach(println)
  }

  /**
    * 将每个分区里面的元素通过seqOp和初始值进行聚合，然后用combine函数将每个分区的结果和初始值(zeroValue)进行combine操作。这个函数最终返回的类型不需要和RDD中元素类型一致
    */
  def aggregate():Unit={
    val source = sc.parallelize(1 to 100,2)
    val result = source.aggregate(0)(_+_,_+_)
    println(result)
  }

  def main(args: Array[String]): Unit = {
    aggregate()
  }

}
