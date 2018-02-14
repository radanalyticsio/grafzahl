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

import scala.collection.immutable._

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
  private val jsonMessageConverter: AMQPJsonFunction = new AMQPJsonFunction()
  private var infinispanHost: String = "datagrid-hotrod"
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
    master = "spark://sparky:7077"

    val ssc = StreamingContext.getOrCreate(checkpointDir, createStreamingContext)
    
    ssc.start()
    ssc.awaitTerminationOrTimeout(batchIntervalSeconds * 1000 * 1000)
    ssc.stop()
  }

  def messageConverter(message: Message): Option[String] = {

    message.getBody match {
      case body: Data => {
        val itemID: String = new String(body.getValue.getArray)
        Some(itemID)
      }
      case body: AmqpValue => {
        val itemID: String = body.asInstanceOf[AmqpValue].getValue.asInstanceOf[String]
        Some(itemID)
      }
      case _ => None
    }
  }

  def storeSale(itemID: String): String = {
    val builder: ConfigurationBuilder = new ConfigurationBuilder()
    builder.addServer().host(infinispanHost).port(infinispanPort)
    
    val cacheManager = new RemoteCacheManager(builder.build())

    val cache= cacheManager.getCache[String, Integer]()

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
 
  def createStreamingContext(): StreamingContext = {
    val ttk = TopK
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    //val conf = new SparkConf().setAppName(appName)
    conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    
    val ssc = new StreamingContext(conf, Seconds(batchIntervalSeconds))
    ssc.checkpoint(checkpointDir)
    
    val receiveStream = AMQPUtils.createStream(ssc, amqpHost, amqpPort, username, password, address, messageConverter _, StorageLevel.MEMORY_ONLY)
    
//    val saleStream = receiveStream.map(storeSale)
    
    val saleStream = receiveStream.foreachRDD{ rdd =>
      rdd.foreach { record =>
        storeSale(record)
        ttk+record
      }
    }
//    saleStream.print()
    ssc
  }
}
