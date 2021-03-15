package streaming_project

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.JsonMethods
import org.json4s.JsonDSL._
import redis.clients.jedis.Jedis
object Job {

  def main(args: Array[String]): Unit = {

    val config = new SparkConf().setAppName("logg").setMaster("local[*]")
    val streamingContext = new StreamingContext(config, Seconds(5))
    streamingContext.checkpoint("E:\\code\\scalaCode\\sparkDemo\\checkpoint")

    val dStream = MyKafkaUtils.getDStreaming(streamingContext, "ad")

    val record: DStream[(String, String, String, String, String)] = dStream.map(x => {
      val arr = x.value().split(", ")
      (arr(0), arr(1), arr(2), arr(3), arr(4))
    })

    Job.top3(record)

    Job.visitNum(record)

    streamingContext.start()
    streamingContext.awaitTermination()

  }

  // 实时top3
  // 时间戳, 地区, 城市, 广告id, 用户id
  def top3(record: DStream[(String, String, String, String, String)]) = {

    val tmpStreaming = record
      .map( x => ((x._3, x._4), 1))   // ((城市, 广告id), 1)
      .updateStateByKey((seq: Seq[Int], state: Option[Int]) => {  // 整个批次计数，  ((城市, 广告id), 次数)
        Some( seq.sum + state.getOrElse(0) )
      })
      .map{   // (城市, (广告id, 次数))
        case ((city, adId), num) => {
          (city, (adId, num))
        }
      }
      // .map(x => (x._1._1, (x._1._2, x._2)))   // (城市, (广告id, 次数))  -- 等价于上面的map，对元组取了别名

    val tmp = tmpStreaming
      .groupByKey()   // (城市, [(广告id, 次数), (广告id, 次数), (广告id, 次数)..])
      .map{
        case (city, iter) => {
          val list = iter.toList
          val sortedList = list.sortBy(-_._2).take(3)
          (city, sortedList)
        }
      }
      // 和上面的写法等价 -- 对元组取了别名
      // .map( x => {
      //   val list = x._2.toList
      //   val sortedList = list.sortBy(_._2).take(3)
      //   (x._1, sortedList)
      // })

    tmp.foreachRDD( rdd => {
      rdd.foreachPartition( iter => {
        val jedis = new Jedis("baseCentOS", 6379)
        iter.foreach {
          case (city, list) => {
            // import org.json4s.JsonDSL._  这个要导入
            jedis.hset("area:top3", city, JsonMethods.compact(list))
          }
        }
        // 和上面的写法等价
        // iter.foreach( item => {
        //   jedis.hset("area:top3", item._1, JSON.toJSONString(item._2))
        // })
        jedis.close()
      })
    })
  }


  // 每隔5秒钟统计一次，当前10分钟内所有广告的访问次数，写入到redis中
  // 时间戳, 地区, 城市, 广告id, 用户id
  def visitNum(record: DStream[(String, String, String, String, String)]) = {
    val tempStreaming = record
      .map( x => {((x._4, x._1), 1)})  // ((广告id, 时间戳), 1)
      .map{     // ((广告id, 小时:分钟), 1)
        case ((adId, time), num) => {
          val date = new Date(time.toLong)
          val dateStr = new SimpleDateFormat("HH:mm").format(date)
          ((adId, dateStr), num)
        }
      }
      .reduceByKeyAndWindow( (x1, x2) => {x1 + x2},   // ((广告id, 时间), 次数)
        (x1, x2) => {x1 - x2},
        Seconds(5 * 12 * 10), Seconds(5))
      .map {        // (广告id, (时间, 次数))
        case ((adId, dateStr), num) => {
          (adId, (dateStr, num))
        }
      }
      .groupByKey()   // (广告id, [(时间, 次数), (时间, 次数), (时间, 次数) ...])

    tempStreaming.foreachRDD( rdd => {
      rdd.foreachPartition( iter => {
        val jedis = new Jedis("baseCentOS", 6379)
        iter.foreach{
          case (adId, list) => {
            jedis.hset("curr_5_min", adId, JsonMethods.compact(list))
          }
        }
        jedis.close()
      })
    })

  }

}
