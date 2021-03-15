package spark_streaming

import java.sql.DriverManager

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Test {

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
      (x.key(), x.value())
    })

    streaming.window(Seconds(10), Seconds(5)).foreachRDD(rdd => {
      rdd.flatMap( x => x._2.split(" "))
        .map( x => (x, 1))
        .reduceByKey( (x1, x2) => x1 + x2)
        .foreach(println)
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
