package com.atguigu.gmall0715.realtimes.util

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

object MyEsUtil {
  private val ES_HOST = "http://hadoop102"
  private val ES_HTTP_PORT = 9200
  private var factory:JestClientFactory = null

  /**
    * 获取客户端
    *
    * @return jestclient
    */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
    * 关闭客户端
    */
  def close(client: JestClient): Unit = {
    if ( client!=null) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }




  /**
    * 建立连接
    */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build)

  }

  def insertEsBulk(indexName:String, sourceList: List[(String,Any)] ): Unit ={
    val jest: JestClient = getClient

    val bulkBuilder = new Bulk.Builder
    for ((id,source) <- sourceList ) {
       val index: Index = new Index.Builder(source).index(indexName).`type`("_doc").id(id).build()
        bulkBuilder.addAction(index)
    }

    val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulkBuilder.build()).getItems
      //一次批量提交es
    println(  "已保存："+items.size()+"条数据")

    close(jest)
  }
//  def main(args: Array[String]): Unit = {
//    val jest: JestClient = getClient
//
//    val index: Index = new Index.Builder(Stud("zhang3","zhang33")).index("gmall2019_stud").`type`("_doc").id("stu123").build()
//    jest.execute(index)
//
//  }
//
//
//  case class Stud(name:String ,nickname:String){
//
//  }


}
