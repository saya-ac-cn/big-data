package ac.cn.saya.bigdata.spark
import org.apache.spark.{SparkConf, SparkContext}
/**
  * @Title: PairRDD
  * @ProjectName big-data
  * @Description: TODO
  * @Author Pandora
  * @Date: 2020/9/2 16:53
  * @Description: 键值对RDD操作
  */
object PairRDD {

  val conf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
  val sc = new SparkContext(conf)

  /**
    * 重新分区
    * 如果原有的partionRDD和现有的partionRDD是一致的话就不进行分区， 否则会生成ShuffleRDD，即会产生shuffle过程
    */
  def partitionBy():Unit={
    val source1 = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c"),(4,"d")),4)
    println("原来的分区数："+source1.partitions.size)
    val source2 = source1.partitionBy(new org.apache.spark.HashPartitioner(2))
    println("新的分区数："+source2.partitions.size)
  }

  /**
    * groupByKey也是对每个key进行操作
    * 可以用于统计词频
    */
  def groupByKey():Unit={
    val inputFile =  "file:///Users/liunengkai/project/java/big-data/spark/src/main/resources/wordcount.txt"
    val textFile = sc.textFile(inputFile)
    // 使用空格切分成一个一个的单词
    val words = textFile.flatMap(line => line.split(" "))
    // 为每个单词生成一个 类似Map的键值对，key=单词，value=1
    val source = words.map(item => (item,1))
    // 针对单词进行分组
    val group = source.groupByKey()
    // 统计每个key中，即每个单词出现的次数
    val count = group.map(item => (item._1,item._2.sum))
    count.saveAsTextFile("file:///Users/liunengkai/project/java/big-data/spark/src/main/resources/wordcount-result.txt")
    count.take(10000).foreach(println)
  }

  /**
    * 词频统计另类用法
    */
  def reduceByKey():Unit={
    //val words = sc.parallelize(List(("one",1), ("two",1), ( "two",1), ( "three",1), ( "three",1), ( "three",1)))
    //words.reduceByKey((x,y) => x + y)
    val words = sc.parallelize(Array("one", "two", "two", "three", "three", "three","two", "three", "three", "one","two", "two"))
    val source = words.map(x => (x,1))
    val newRdd = source.reduceByKey((x1,x2) => x1+x2)
    newRdd.take(10000).foreach(println)
  }

  def aggregateByKey():Unit={
    val source = sc.parallelize(List(("a",1),("a",2),("a",3),("b",4),("b",5),("c",6)),3)
    // 创建一个pairRDD，取出每个分区相同key对应值的最大值，然后相加
    val newRdd = source.aggregateByKey(0)(math.max(_,_),_+_)
    newRdd.take(10000).foreach(println)
  }

  def foldByKey():Unit={
    val source = sc.parallelize(List(("a",1),("a",2),("a",3),("b",4),("b",5),("c",6)),3)
    // 创建一个pairRDD，取出每个分区相同key，然后相加
    val newRdd = source.foldByKey(0)(_+_)
    newRdd.take(10000).foreach(println)
  }

  /**
    * 对相同K，把V合并成一个集合
    */
  def combineByKey():Unit={
    // 创建一个pairRDD
    val source = sc.parallelize(List(("a",1),("a",2),("a",3),("b",4),("b",5),("c",6),("b",4),("b",5),("c",6),("a",1),("a",2),("a",3)),3)
    // 将相同key对应的值相加，同时记录该key出现的次数，放入一个二元组
    val group = source.combineByKey((_,1),(item:(Int,Int),value) =>(item._1+value,item._2+1),(item1:(Int,Int),item2:(Int,Int))=>(item1._1+item2._1,item1._2+item2._2))
    val result = group.map(item => (item._1,item._2._1/item._2._2.toDouble))
    result.take(10000).foreach(println)
  }

  def sortByKey():Unit={
    val source = sc.parallelize(Array((3,"a"),(4,"c"),(2,"b"),(1,"d")))
    // 升序
    val result1 = source.sortByKey(true)
    result1.take(10000).foreach(println)
    // 降序
    val result2 = source.sortByKey(false)
    result2.take(10000).foreach(println)
  }

  def mapValues():Unit={
    // 创建一个pairRDD
    val source = sc.parallelize(List(("a",1),("a",2),("a",3),("b",4),("b",5),("c",6),("b",4),("b",5),("c",6),("a",1),("a",2),("a",3)),3)
    val result = source.mapValues("'"+_.toString+"'")
    result.take(10000).foreach(println)
  }

  def join():Unit={
    val source1 = sc.parallelize(List(("a",1),("b",3),("c",4)))
    val source2 = sc.parallelize(List(("b",2),("c",5),("d",8)))
    val result = source1.join(source2)
    result.take(10000).foreach(println)
  }

  def cogroup():Unit={
    val source1 = sc.parallelize(Array(("a",1),("b",3),("c",4),("a",1),("b",3)))
    val source2 = sc.parallelize(Array(("b",2),("c",5),("d",8),("c",5),("d",8)))
    val result = source1.join(source2)
    result.take(10000).foreach(println)
  }

  def main(args: Array[String]): Unit = {
    cogroup()
  }

}
