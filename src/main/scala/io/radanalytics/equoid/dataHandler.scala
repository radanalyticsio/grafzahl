package io.radanalytics.equoid

import java.lang.Long

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.radanalytics.streaming.amqp.AMQPJsonFunction
import io.vertx.core.{AsyncResult, Handler, Vertx}
import io.vertx.proton._
import org.apache.log4j.{Level, Logger}
import org.apache.qpid.proton.amqp.messaging.{AmqpValue, Data}
import org.apache.qpid.proton.message.Message
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.amqp.AMQPUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.util.Random

import org.infinispan._
import org.infinispan.client.hotrod.RemoteCache
import org.infinispan.client.hotrod.RemoteCacheManager
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.impl.ConfigurationProperties


object dataHandler {

  private val master: String = "local[2]"
  private val appName: String = getClass().getSimpleName()

  private val batchIntervalSeconds: Int = 1
  private val checkpointDir: String = "/tmp/equoid-data-handler"

  private var amqpHost: String = "172.17.0.6"
  private var amqpPort: Int = 5672
  private var address: String = "salesq"
  private var username: Option[String] = Option("daikon")
  private var password: Option[String] = Option("daikon")
  private val jsonMessageConverter: AMQPJsonFunction = new AMQPJsonFunction()
  private var infinispanHost: String = "172.30.149.192"
  private var infinispanPort: Int = 11333

  def main(args: Array[String]): Unit = {

    if (args.length < 7) {
      System.err.println("Usage: dataHandler <AMQHostname> <AMQPort> <AMQUsername> <AMQPassword> <AMQQueue> <JDGHostname> <JDGPort>")
      System.exit(1)
    }

    amqpHost = args(0)
    amqpPort = args(1).toInt
    username = Option(args(2))
    password = Option(args(3))
    address = args(4)
    infinispanHost = args(5)
    infinispanPort = args(6).toInt

    val ssc = StreamingContext.getOrCreate(checkpointDir, createStreamingContextJson)
    
    ssc.start()
    ssc.awaitTerminationOrTimeout(batchIntervalSeconds * 1000 * 1000)
    ssc.stop()
  }

  def getSale(jsonMsg: String): String = {
    val mapper: ObjectMapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val node: JsonNode = mapper.readTree(jsonMsg)
    node.get("body").get("section").toString
  }

  def storeSale(itemID: String): Unit = {
    val builder: ConfigurationBuilder = new ConfigurationBuilder()
    builder.addServer().host(infinispanHost).port(infinispanPort)
    val cacheManager = new RemoteCacheManager(builder.build())

    var cache: RemoteCache[String, String] = cacheManager.getCache()

    var ret = cache.get(itemID)
    if (ret!=null) {
      ret = (ret.toInt+1).toString
    }
    else {
      ret = "1"
    }
    cache.put(itemID, ret)
    cacheManager.stop()
  }
  
 
  def messageConverter(message: Message): Option[String] = {
    message.getBody match {
      case body: Data => {
        val itemID: String = new String(body.getValue.getArray)
        Some(itemID)
      }
      case _ => None
    }
  }

  def createStreamingContextJson(): StreamingContext = {
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    val ssc = new StreamingContext(conf, Seconds(batchIntervalSeconds))
    ssc.checkpoint(checkpointDir)

    val receiveStream = AMQPUtils.createStream(ssc, amqpHost, amqpPort, username, password, address, jsonMessageConverter, StorageLevel.MEMORY_ONLY)
    
    val saleStream = receiveStream.map(getSale)
    saleStream.foreachRDD{ rdd =>
      rdd.foreach { record => 
        storeSale(record)
      }
    }
    ssc
  }
}
