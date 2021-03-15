package spark_rdd

import java.sql.{Driver, DriverManager}

import org.apache.spark.{SparkConf, SparkContext}

object WriteRDDToMysql {

  def main(args: Array[String]): Unit = {
    val sparkConfig = new SparkConf()
    sparkConfig.setAppName("spark_mysql")
    sparkConfig.setMaster("local[*]")

    val sparkContext = new SparkContext(sparkConfig)

    val data = sparkContext.makeRDD(Array(("张三", 22), ("李思思", 21)), 2)

    // 连接放到算子外就报错了 --  不能序列化
    // 分布式计算，需要将这个连接发送到每一台服务器上，需要进行序列化
    // val conn = DriverManager.getConnection("jdbc:mysql://192.168.157.129:3306/spark", "root", "123456")

    // 这里的代码是再每一台机器上运行，因此在这里创建连接
    data.foreachPartition(iter => {
      // 在每个分区中创建一个连接（每个executor创建一个）
      val conn = DriverManager.getConnection("jdbc:mysql://192.168.157.129:3306/spark", "root", "123456")
      iter.foreach( x => {
        val ps = conn.prepareStatement("insert into spark_user(name, age) values (?, ?)")
        ps.setString(1, x._1)
        ps.setInt(2, x._2)
        ps.execute()
        ps.close()
      })
      conn.close()
    })

  }
}
