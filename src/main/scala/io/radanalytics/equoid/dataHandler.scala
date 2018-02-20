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

import org.infinispan._
import org.infinispan.client.hotrod.RemoteCache
import org.infinispan.client.hotrod.RemoteCacheManager
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.impl.ConfigurationProperties

import scala.collection.immutable

object dataHandler {

  private var master: String = "local[2]"
  private val appName: String = getClass().getSimpleName()

  private val batchIntervalSeconds: Int = 1
  private val checkpointDir: String = "/tmp/equoid-data-handler"

  private var amqpHost: String = "localhost"
  private var amqpPort: Int = 5672
  private var address: String = "salesq"
  private var username: Option[String] = Option("daikon")
  private var password: Option[String] = Option("daikon")
  private var infinispanHost: String = "datagrid-hotrod"
  private var infinispanPort: Int = 11333
  
  private var k: Int = 10
  private var epsilon: Double = 6.0
  private var confidence: Double = 0.9

  def main(args: Array[String]): Unit = {

    if (args.length < 10) {
      System.err.println("Usage: dataHandler <AMQHostname> <AMQPort> <AMQUsername> <AMQPassword> <AMQQueue> <JDGHostname> <JDGPort> <k> <epsilon> <confidence>")
      System.exit(1)
    }

    amqpHost = args(0)
    amqpPort = args(1).toInt
    username = Option(args(2))
    password = Option(args(3))
    address = args(4)
    infinispanHost = args(5)
    infinispanPort = args(6).toInt
    k = args(7).toInt
    epsilon = args(8).toDouble
    confidence = args(9).toDouble
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

  def storeSale(itemID: String): String = {
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
  
  def storeTopK(topk: immutable.Map[String, Int]): Unit = {
    val builder: ConfigurationBuilder = new ConfigurationBuilder()
    builder.addServer().host(infinispanHost).port(infinispanPort)
    val cacheManager = new RemoteCacheManager(builder.build())
    val cache = cacheManager.getCache[String, Integer]()
    for ((key,v) <- topk) cache.put(key, v) 
    cacheManager.stop()
  }

  def createStreamingContext(): StreamingContext = {
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
      })
      storeTopK(globalTopK.topk)
    })
    ssc
  }
}
