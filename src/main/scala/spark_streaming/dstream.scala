package spark_streaming
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object DStream {

  def main(args: Array[String]): Unit = {

    // 1. 创建配置，和context对象 -- 一个executor用于收集数据，一个用于计算，因此最少需要2个executor
    val conf = new SparkConf().setAppName("streaming").setMaster("local[*]")
    // 处5秒内产生的数据
    val context = new StreamingContext(conf, Seconds(5))

    // 2. 设置数据源
    // 监听baseCentOS这个主机，9999端口接收到的数据 -- 是一个DStream对象
    // DStream -- 类似于RDD
    val streaming: DStream[String] = context.socketTextStream("baseCentOS", 9999)

    // 3. 计算数据
    val streamingWordCount = streaming.flatMap(x=>{x.split(" ")})
      .map(x=>(x, 1))
      .reduceByKey((x1, x2)=>{ x1 + x2 })

    // 类似于rdd的行动算子，没有collect这样的行动算子
    streamingWordCount.print()

    // 4. 启动spark streaming程序
    // 启动程序
    context.start()
    // 等待终止 -- 继续处理数据
    context.awaitTermination()


  }
}
