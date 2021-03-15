package spark_counter

import org.apache.spark.{SparkConf, SparkContext}

object Counter {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("hbase_test")
    conf.setMaster("local[*]")
    val sparkContext = new SparkContext(conf)

    val rdd = sparkContext.makeRDD(Array(1, 2, 3, 4, 5))

    var count = sparkContext.longAccumulator("count") // 累加器

    rdd.foreach( x => {
      count.add(x)
    })

    println(s"count: ${count.value}")

  }

}
