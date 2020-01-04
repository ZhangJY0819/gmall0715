package com.atguigu.gmall0715.realtimes.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0715.common.constant.GmallConstant
import com.atguigu.gmall0715.realtimes.util.{MyEsUtil, MyKafkaUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks


object AlertApp {

  case class EventInfo(mid: String,
                       uid: String,
                       appid: String,
                       area: String,
                       os: String,
                       ch: String,
                       `type`: String,
                       evid: String,
                       pgid: String,
                       npgid: String,
                       itemid: String,
                       var logDate: String,
                       var logHour: String,
                       var ts: Long
                      )

  case class CouponAlertInfo(mid: String,
                             uids: java.util.HashSet[String],
                             itemIds: java.util.HashSet[String],
                             events: java.util.List[String],
                             ts: Long)

  /**
    * 预警日志格式：
    * mid  	设备id
    * uids	领取优惠券登录过的uid
    * itemIds	优惠券涉及的商品id
    * events  	发生过的行为
    * ts	发生预警的时间戳
    *
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("alter_app").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //格式转换为样例类
    val eventInfoDstream: DStream[EventInfo] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_EVENT, ssc)
      .map { record =>
        val jsonstr: String = record.value()
        val enentInfo: EventInfo = JSON.parseObject(jsonstr, classOf[EventInfo])
        enentInfo
      }
    //开窗口
    val eventInfoWindowDstream: DStream[EventInfo] = eventInfoDstream.window(Seconds(300), Seconds(10))
    //对同一mid分组
    val groupbyMidDstream: DStream[(String, Iterable[EventInfo])] = eventInfoWindowDstream.map(eventInfo => (eventInfo.mid, eventInfo)).groupByKey()

    //判断预警
    //在一个设备内
    //  1.出现三次及三次以上的uid领取优惠券
    //  2.没有浏览过商品（evid clickItem）
    val alertInfoDstream: DStream[(Boolean, CouponAlertInfo)] = groupbyMidDstream.map { case (mid, eventInfoItr) =>
      //创建java的set集合用来存放uid
      val uidsSet = new util.HashSet[String]()
      //用来存放优惠券涉及的商品id
      val itemSet = new util.HashSet[String]()
      //用；list集合存放发生过的行为
      val eventList = new util.ArrayList[String]()
      var ifAlert = false
      var isClickItem = false

      Breaks.breakable(
        for (eventInfo: EventInfo <- eventInfoItr) {
          eventList.add(eventInfo.evid)
          if (eventInfo.evid == "coupon") {
            //保存领取中的登录账号
            uidsSet.add(eventInfo.uid)
            itemSet.add(eventInfo.itemid)
          }
          if (eventInfo.evid == "clickItem") { //判断是否有点击商品
            isClickItem = true
            Breaks.break
          }
        }
      )

      if (uidsSet.size() >= 3 && isClickItem == false) {
        ifAlert = true
      }
      (ifAlert, CouponAlertInfo(mid, uidsSet, itemSet, eventList, System.currentTimeMillis()))
    }
    val filteredDstream = alertInfoDstream.filter(_._1)


    filteredDstream.foreachRDD{rdd=>
      rdd.foreachPartition{alertItr =>
        // 6 同一设备，每分钟只记录一次预警 去重
        // 利用要保存到的数据库的 幂等性进行去重  PUT
        // ES的幂等性 是基于ID    设备+分钟级时间戳作为id

        val sourceList = alertItr.map{case(flag,alertInfo)=>
          (alertInfo.mid+"_"+alertInfo.ts/1000/60,alertInfo)
        }.toList
        MyEsUtil.insertEsBulk(GmallConstant.ES_INDEX_ALERT,sourceList)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
