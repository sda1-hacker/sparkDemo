package spark_rdd

import java.util.Date

import org.apache.spark.{SPARK_BRANCH, SparkConf, SparkContext}


object SparkRddTest {

  def main(args: Array[String]): Unit = {

    val sparkConfig = new SparkConf()
    sparkConfig.setAppName("spark_demo1")
    sparkConfig.setMaster("local[*]")

    val sparkContext = new SparkContext(sparkConfig)

    sparkContext.setCheckpointDir("hdfs://192.168.157.129:9000/spark/checkpoint") // 设置checkpoint数据存储的目录

    val rdd = sparkContext.makeRDD(Array("ZhangSiSi"))

    val rdd1 = rdd.map(x => {
      x + System.currentTimeMillis()
    })

    rdd1.cache()  // 做缓存

    // 调用行动算子的时候才会将数据缓存到HDFS中。
    // 在第一次调用完行动算子之后会重新进行一次计算（重新计算是为了做checkpoint）。
    // 如果做了cache()，那么这次重新计算就会从缓存中读取数据。
    // 因此在做checkpoint之前，需要将计算的结果缓存到内存中。
    rdd1.checkpoint()

    rdd1.foreach(x => {println(x)})
    rdd1.foreach(x => {println(x)})


  }

}


