package com.atguigu.gmall0715.realtimes.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0715.common.constant.GmallConstant
import com.atguigu.gmall0715.realtimes.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}


object AlertApp {
  case class EventInfo(mid:String,
                       uid:String,
                       appid:String,
                       area:String,
                       os:String,
                       ch:String,
                       `type`:String,
                       evid:String ,
                       pgid:String ,
                       npgid:String ,
                       itemid:String,
                       var logDate:String,
                       var logHour:String,
                       var ts:Long
                      )
  case class CouponAlertInfo(mid:String,
                             uids:java.util.HashSet[String],
                             itemIds:java.util.HashSet[String],
                             events:java.util.List[String],
                             ts:Long)

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
    val ssc = new StreamingContext(conf,Seconds(5))
    //格式转换为样例类
    val eventInfoDstream: DStream[EventInfo] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_EVENT, ssc)
      .map { record =>
        val jsonstr: String = record.value()
        val enentInfo: EventInfo = JSON.parseObject(jsonstr, classOf[EventInfo])
        enentInfo
      }
    //开窗口
    val eventInfoWindowDstream: DStream[EventInfo] = eventInfoDstream.window(Seconds(300),Seconds(10))
    //对同一mid分组
    val groupbyMidDstream: DStream[(String, Iterable[EventInfo])] = eventInfoWindowDstream.map(eventInfo=>(eventInfo.mid,eventInfo)).groupByKey()

    //判断预警
    //在一个设备内
    //  1.出现三次及三次以上的uid领取优惠券
    //  2.没有浏览过商品（evid clickItem）
    groupbyMidDstream.map{case (mid,eventInfoItr)=>
        //创建java的set集合用来存放uid
      val couponUidsSet = new util.HashSet[String]()
        //用来存放优惠券涉及的商品id
      val itemIdsSet = new util.HashSet[String]()
        //用；list集合存放发生过的行为
      val eventIds = new util.ArrayList[String]()


    }
  }
}
