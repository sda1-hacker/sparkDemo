package spark_demo1

import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("spark_demo1")  // 应用名，可以重复，spark后台维护了一个appid
      .setMaster("local") // local本地模式 -- 启动一个Executor
      // .setMaster("local[*]") // local本地模式 -- 启动的Executor个数和cpu个数相同
      // .setMaster("local[3]") // local本地模式 -- 启动3个Executor

      // .setMaster("yarn") // yarn模式   -- 只能打jar在服务器上运行
      // .setMaster("spark://192.168.157.129:7077")     // standalone模式 -- 这种模式写这个url -- 只能打jar在服务器上运行

    // 创建context对象
    val context = new SparkContext(conf)
    // val text = context.textFile("hdfs://192.168.157.129:9000/spark/word.txt")

    val text = context.makeRDD(List("hello world hello mem hello nihao"))

    val res = text
      // 转换算子
      .flatMap(_.split(" "))  // .flatMap(text => text.split(" "))
      .map((_,1))                    // .map(text => (text, 1))
      .groupBy(_._1)                 // .groupBy(text => text)
      .map(group => (group._1, group._2.size))
      .sortBy(_._2)
      // 行动算子 --
      .collect()

    for (tmp <- res) {
      println(tmp)
    }

    // 关闭context对象
    context.stop()

  }

}
