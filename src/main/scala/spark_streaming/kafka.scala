package spark_streaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object kafka {
  def main(args: Array[String]): Unit = {

    val kafka_node = "baseCentOS:9092"

    val kafka_group = "g1"

    val kafka_topic = "wordCount"

    val conf = new SparkConf().setAppName("kafka").setMaster("local[*]")

    val context = new StreamingContext(conf, Seconds(5))

    val kafka_params = Map[String, String](
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> kafka_group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafka_node
    )

    // 从kafka中读取数据，创建DStream
    // 第一个参数：context
    // 第二个参数：位置策略,
    //  LocationStrategies.PreferConsistent： kafka的分区会均匀的分布在executor上，推荐使用
    //  LocationStrategies.PreferBrokers： kafka和executor在相同的机器上，executor中的数据会来自当前的kafka节点， executor和kafka在相同机器上使用这个策略
    //  LocationStrategies.PreferFixed： 可以通过map指定将topic分区分布在哪些节spark点中
    // 第三个参数：消费者策略
    val record = KafkaUtils.createDirectStream(context, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](List(kafka_topic), kafka_params)) // 指定了消费数据key value的类型

    record
      .map(x => {x.value()})
      .flatMap(x => {x.split(" ")})
      .map(x => {(x, 1)})
      .reduceByKey((x1, x2) => {x1 + x2})
      .print()



    context.start()
    context.awaitTermination()
  }
}
