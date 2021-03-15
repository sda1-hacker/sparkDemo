package streaming_project

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object MyKafkaUtils {

  val kafkaConsumerParams = Map[String, String](
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.GROUP_ID_CONFIG -> "g1",
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "baseCentOS:9092"
  )


  // 从kafka中获取信息到DStream
  def getDStreaming(streamingContext: StreamingContext, topic: String) = {
    KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](List(topic), kafkaConsumerParams))
  }


  // 创建生产者
  def getProducer() = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "baseCentOS:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String, String](props)
  }


  // 模拟产生数据
  // 数据格式：时间戳, 地区, 城市, 广告id, 用户id
  def loadData() = {
    val city = Map[Int, String](
      0 -> "华南, 广州",
      1 -> "华北, 北京",
      2 -> "华北, 天津",
      3 -> "华东, 上海",
      4 -> "华北, 河北",
      5 -> "华中, 湖北",
      6 -> "华中, 河南"
    )

    val recordNum = Random.nextInt(10) + 1
    val recordList = ArrayBuffer[String]()
    for (num <- 1 to recordNum) {
      val currTime = System.currentTimeMillis()
      val area = city.get(Random.nextInt(7)).getOrElse("中国, 中国")
      val adId = Random.nextInt(200) + 50
      val userId = Random.nextInt(10) + 1

      val record = currTime + ", " + area + ", " + adId + ", " + userId
      recordList += record
    }
    recordList
  }


  // 模拟发送数据
  def sendMessage(topic: String): Unit = {

    // 模拟的数据
    val producer = MyKafkaUtils.getProducer()
    while (true) {
      val dataList = MyKafkaUtils.loadData()
      dataList.foreach(data => {
        producer.send(new ProducerRecord[String, String](topic, data))
        Thread.sleep(100)
      })
      Thread.sleep(1000)
    }
  }

  def main(args: Array[String]): Unit = {
    MyKafkaUtils.sendMessage("ad")
  }
}
