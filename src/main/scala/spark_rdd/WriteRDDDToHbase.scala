package spark_rdd

import java.util

import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

object WriteRDDDToHbase {

  def main(args: Array[String]): Unit = {
    val sparkConfig = new SparkConf()
      .setAppName("hbase_test")
      .setMaster("local[*]")

    val sparkContext = new SparkContext(sparkConfig)

    val rdd = sparkContext.makeRDD(Array(
      ("1", "zhangSan", "20", "2500"),
      ("2", "liSiSi", "18", "2600"),
      ("3", "wuWu", "21", "2400"),
      ("4", "zhao66", "20", "2500")
    ), 2)

    // 写入的时候会卡在这里。不知道为啥。 --  之后看看有没有更好的办法。
    rdd.foreachPartition( iter => {

      val conf = HBaseConfiguration.create()
      conf.set(HConstants.ZOOKEEPER_QUORUM, "baseCentOS")
      val conn = ConnectionFactory.createConnection(conf)
      val table = conn.getTable(TableName.valueOf("spark_db:spark_test"))
      val puts = new util.ArrayList[Put]()

      iter.foreach(x => {

        val put = new Put(Bytes.toBytes(x._1)) // rowkey
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(x._2))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(x._3))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("salary"), Bytes.toBytes(x._4))
        puts.add(put)

      })
      table.put(puts)
      table.close()
    })

    sparkContext.stop()
  }
}
