package com.atguigu.gmall0715.realtimes.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0715.common.constant.GmallConstant
import com.atguigu.gmall0715.realtimes.bean.StartUpLog
import com.atguigu.gmall0715.realtimes.util.{MyKafkaUtil,RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._
object DauApp {


  //1、消费kafka的数据
  //2、json字符串 -> 转换为一个对象 case class
  //3、利用redis进行过滤
  //4、把过滤后的新数据进行写入redis(当日用户访问的清单)
  //5、再把数据写入到hbase中
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("dau_app")

    val ssc = new StreamingContext(conf, Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP, ssc)

    inputDstream.map(_.value()).print()

    //2、json字符串 -> 转换为一个对象 case class
    val startUpDstream: DStream[StartUpLog] = inputDstream.map {
      record => {
        val jsonString: String = record.value()
        val startUpLog: StartUpLog = JSON.parseObject(jsonString, classOf[StartUpLog])

        val datetime: Date = new Date(startUpLog.ts)
        val formator = new SimpleDateFormat("yyyy-MM-dd HH")
        val datetimeStr: String = formator.format(datetime)
        val datetimeArr = datetimeStr.split(" ")

        startUpLog.logDate = datetimeArr(0)
        startUpLog.logHour = datetimeArr(1)

        startUpLog
      }
    }

    //3 利用广播变量 把清单发给各个executor 各个executor根据清单进行比对进行过滤
    val filterDstream: DStream[StartUpLog] = startUpDstream.transform {rdd =>
      val jedis = RedisUtil.getJedisClient
      val format = new SimpleDateFormat("yyyy-MM-dd")
      val today: String = format.format(new Date())
      val dauKey: String = "dau:" + today
      val dauSet: util.Set[String] = jedis.smembers(dauKey)
      jedis.close()
      val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
      println("过滤前："+rdd.count()+"条")

      val filetrRDD: RDD[StartUpLog] = rdd.filter {startup =>

          val dauSet: util.Set[String] = dauBC.value
          !dauSet.contains(startup.mid)
      }
      println("过滤后："+rdd.count()+"条")
      filetrRDD
    }

    //本批次 自检去重
    // 相同的mid 保留第一条
    val groupbyMidDStream: DStream[(String, Iterable[StartUpLog])] = filterDstream.map(startuplog=>(startuplog.mid,startuplog)).groupByKey()
    val filteredSefDstream: DStream[StartUpLog] = groupbyMidDStream.map { case (mid, startupLogItr) =>
      val top1list: List[StartUpLog] = startupLogItr.toList.sortWith((startupLog1, startupLog2) => startupLog1.ts < startupLog2.ts).take(1)
      top1list(0)
    }

    filteredSefDstream.cache()

    //4、把过滤后的新数据进行写入redis（当日用户访问的清单）
    filteredSefDstream.foreachRDD {
      rdd => {
        rdd.foreachPartition {
          startupItr =>
            val jedis: Jedis = RedisUtil.getJedisClient
            for (startup <- startupItr) {
              //保存当日的用户清单
              //jedis type: set  key: dau:[日期]  value: mid
              val dauKey: String = "dau:"+startup.logDate
              jedis.sadd(dauKey,startup.mid)
            }
          jedis.close()
        }
      }
    }

    //5、再把数据写入到hbase中
    filteredSefDstream.foreachRDD{rdd =>
      rdd.saveToPhoenix("GMALL0715_DAU", Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
