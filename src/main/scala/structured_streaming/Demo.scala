package structured_streaming

import java.sql.{DriverManager, Timestamp}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession, functions}
import org.apache.spark.sql.streaming.OutputMode

object Demo {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder()
      .appName("demo")
      .master("local[*]")
      // http://spark.apache.org/docs/latest/configuration.html 配置文件
      .config("spark.sql.shuffle.partitions", 10) // 分区个数，默认200，用于处理大量数据
      .getOrCreate()

    import sparkSession.implicits._

    // http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
    val dataFrame = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "baseCentOS:9092")   // zk地址
      .option("subscribe", "wordCount")   // topic
      .load()
      .selectExpr("cast(value as string)").as[String]    // kafka读取的是consumerRecord对象，这句话的意思是将value属性取出作为一个字符串


    // 数据格式：    单词, 时间 -- 例如： hello, 2021-3-10 19:33:21    时间戳也可以，这样方便观察
    val dataFrame2 = dataFrame.map( line => {
      val arr = line.split(", ")
      (arr(0), Timestamp.valueOf(arr(1)))     // 这里需要转成时间戳。
    }).toDF("words", "timestamp")


    val dataFrame3 = dataFrame2.withWatermark("timestamp", "10 seconds")
      .select(functions.window($"timestamp", "10 seconds", "10 seconds"), $"words", $"timestamp")

    dataFrame3.createOrReplaceTempView("t_word")

    var sql =
      """
        |select words, window, count(*) as num from t_word
        |group by window, words
        |""".stripMargin

    val dataFrame4 = sparkSession.sql(sql)

    val query = dataFrame4
      .writeStream
      .outputMode(OutputMode.Update())
      .option("truncate", false)    // 不截断
      .format("console")
      .start()

    query.awaitTermination()

  }

}
