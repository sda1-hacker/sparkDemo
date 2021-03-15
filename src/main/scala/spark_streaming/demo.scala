package spark_streaming

import java.sql.DriverManager

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object demo {

  def main(args: Array[String]): Unit = {

    val kafka_node = "baseCentOS:9092"

    val kafka_group = "g1"

    val kafka_topic = "wordCount"

    val conf = new SparkConf().setAppName("kafka").setMaster("local[*]")

    val streamingContext = new StreamingContext(conf, Seconds(5))

    streamingContext.checkpoint("E:\\code\\scalaCode\\sparkDemo\\checkpoint")

    val context = streamingContext.sparkContext

    val kafka_params = Map[String, String](
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> kafka_group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafka_node
    )

    val record = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](List(kafka_topic), kafka_params)) // 指定了消费数据key value的类型

    val streaming = record.map(x => {
      x.value()
    })

    streaming
      // 通过transform获取到rdd, 直接对rdd进行转换操作
      .transform(rdd => {
        rdd
          .flatMap( _.split(" "))
          .map( (_, 1))
          .reduceByKey((x1, x2) => x1 + x2)
      })
      // 通过foreachRDD获取到RDD
      .foreachRDD( rdd => {
        // 在每个executor上操作
        rdd.foreachPartition( iter => {
          val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark", "root", "123456")
          iter.foreach( item => {
            val ps = conn.prepareStatement("insert into spark_word(word, num) values (?, ?)")
            ps.setString(1, item._1)
            ps.setInt(2, item._2)
            ps.execute()
            ps.close()
          })
        })
      })


    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
