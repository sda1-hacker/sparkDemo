package spark_boardcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

object boardCast {

  def test: Unit = {

    val conf = new SparkConf()
    conf.setAppName("hbase_test")
    conf.setMaster("local[*]")
    val sparkContext = new SparkContext(conf)

    val prefix = "三年二班--"
    val rdd = sparkContext.makeRDD(Array("张三", "李思思", "赵66"), 3)

    rdd.map(x => {
      prefix + x
    })

  }

  def test1: Unit ={

    val conf = new SparkConf()
    conf.setAppName("hbase_test")
    conf.setMaster("local[*]")
    val sparkContext = new SparkContext(conf)

    val prefix = "三年二班--"
    val broadCast: Broadcast[String] = sparkContext.broadcast(prefix)

    val rdd = sparkContext.makeRDD(Array("张三", "李思思", "赵66"), 3)

    rdd.map(x => {
      broadCast.value + x
    })

  }
}
