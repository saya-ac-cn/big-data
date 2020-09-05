package ac.cn.saya.bigdata.spark
import org.apache.spark.{SparkConf, SparkContext}
/**
  * @Title: WordCount
  * @ProjectName big-data
  * @Description: TODO
  * @Author Pandora
  * @Date: 2020/9/2 12:01
  * @Description: 单词统计
  */

object WordCount {
  def main(args: Array[String]) {

    val inputFile =  "file:///Users/liunengkai/project/java/big-data/spark/src/main/resources/wordcount.txt"
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
    val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a+b)
    wordCount.foreach(println)
  }
}
