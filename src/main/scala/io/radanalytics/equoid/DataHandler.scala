package io.radanalytics.equoid

import java.lang.Long

import io.radanalytics.streaming.amqp.AMQPJsonFunction
import io.vertx.core.{AsyncResult, Handler, Vertx}
import io.vertx.proton._
import org.apache.log4j.{Level, LogManager, PropertyConfigurator, Logger}

import org.apache.qpid.proton.amqp.messaging.{AmqpValue, Data}
import org.apache.qpid.proton.message.Message
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd
import org.apache.spark.streaming.amqp.AMQPUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import org.apache.spark.util.sketch.CountMinSketch
import io.radanalytics.equoid._
import org.apache.spark.sql.SparkSession

import org.apache.spark.util.sketch
import scala.util.Random
import scala.util.Properties

import org.infinispan._
import org.infinispan.client.hotrod.RemoteCache
import org.infinispan.client.hotrod.RemoteCacheManager
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.impl.ConfigurationProperties

import scala.collection.immutable

object DataHandler {

  private var master: String = "local[2]"
  private val appName: String = getClass().getSimpleName()

  private val batchIntervalSeconds: Int = 1
  private val checkpointDir: String = "/tmp/equoid-data-handler"

  def getProp(camelCaseName: String, defaultValue: String): String = {
    val snakeCaseName = camelCaseName.replaceAll("(.)(\\p{Upper})", "$1_$2").toUpperCase()
    Properties.envOrElse(snakeCaseName, Properties.scalaPropOrElse(snakeCaseName, defaultValue))
  }  
  
  def main(args: Array[String]): Unit = {

    master = "spark://sparky:7077"

    val ssc = StreamingContext.getOrCreate(checkpointDir, createStreamingContext)
    
    ssc.start()
    ssc.awaitTerminationOrTimeout(batchIntervalSeconds * 1000 * 1000)
    ssc.stop()
  }

  def messageConverter(message: Message): Option[String] = {

    message.getBody match {
      case body: AmqpValue => {
        val itemID: String = body.getValue.asInstanceOf[String]
        Some(itemID)
      }
      case x => { println(s"unexpected type ${x.getClass.getName}"); None }
    }
  }

  def storeSale(itemID: String, infinispanHost: String, infinispanPort: Int): String = {
    val builder: ConfigurationBuilder = new ConfigurationBuilder()
    builder.addServer().host(infinispanHost).port(infinispanPort)
    
    val cacheManager = new RemoteCacheManager(builder.build())

    val cache = cacheManager.getCache[String, Integer]()

    var ret = cache.get(itemID)
    if (ret!=null) {
      ret = ret+1
    }
    else {
      ret = 1
    }
    
    cache.put(itemID, ret)
    cacheManager.stop()
    itemID
  }
  
  def storeTopK(topk: immutable.Map[String, Int], infinispanHost: String, infinispanPort: Int): Unit = {
    val builder: ConfigurationBuilder = new ConfigurationBuilder()
    builder.addServer().host(infinispanHost).port(infinispanPort)
    val cacheManager = new RemoteCacheManager(builder.build())
    val cache = cacheManager.getCache[String, Integer]()
    for ((key,v) <- topk) cache.put(key, v) 
    cacheManager.stop()
  }

  def createStreamingContext(): StreamingContext = {
    val amqpHost = getProp("amqpHost", "broker-amq-amqp")
    val amqpPort = getProp("amqpPort", "5672").toInt
    val username = Option(getProp("amqpUsername", "daikon"))
    val password = Option(getProp("amqpPassword", "daikon"))
    val address = getProp("queueName", "salesq")
    val infinispanHost = getProp("jdgHost", "datagrid-hotrod")
    val infinispanPort = getProp("jdgPort", "11333").toInt
    val k = getProp("cmsK", "3").toInt
    val epsilon = getProp("cmsEpsilon", "0.01").toDouble
    val confidence = getProp("cmsConfidence", "0.9").toDouble    
    var globalTopK = TopK.empty[String](k, epsilon, confidence)
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    
    val ssc = new StreamingContext(conf, Seconds(batchIntervalSeconds))
    ssc.checkpoint(checkpointDir)
    
    val receiveStream = AMQPUtils.createStream(ssc, amqpHost, amqpPort, username, password, address, messageConverter _, StorageLevel.MEMORY_ONLY)
   
    val saleStream = receiveStream.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val partitionTopK = partitionOfRecords.foldLeft(TopK.empty[String](k, epsilon, confidence))(_+_)
        globalTopK = globalTopK ++ partitionTopK
        storeTopK(globalTopK.topk, infinispanHost, infinispanPort)
      })
    })
    ssc
  }
}
