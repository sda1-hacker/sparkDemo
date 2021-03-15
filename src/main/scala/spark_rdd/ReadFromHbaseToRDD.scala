package spark_rdd

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

object ReadFromHbaseToRDD {

  def main(args: Array[String]): Unit = {



    val conf = new SparkConf()
    conf.setAppName("hbase_test")
    conf.setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val hadoopConfig = new Configuration()
    hadoopConfig.set(HConstants.ZOOKEEPER_QUORUM, "baseCentOS") // zookeeper
    hadoopConfig.set(TableInputFormat.INPUT_TABLE, "spark_db:spark_test") // hbase中的表

    // 第二个参数 以什么样的方式去读数据
    // 第三个参数 rowKey类型
    // 第四个参数 hbase中数据的类型（除rowKey以为数据的类型）
    val rdd = sparkContext.newAPIHadoopRDD(hadoopConfig, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      .map( x => {
        var rowKey = Bytes.toString(x._1.get())
        // 和javaApi一样
        var name = Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
        var age = Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")))
        var salary = Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("salary")))
        (rowKey, name, age, salary)
      } )

    val res = rdd.collect()
  }
}
