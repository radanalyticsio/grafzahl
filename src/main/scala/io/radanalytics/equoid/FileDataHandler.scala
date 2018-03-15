package io.radanalytics.equoid

import java.lang.Long

import org.apache.log4j.{Level, LogManager, PropertyConfigurator, Logger}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd
import org.apache.spark.streaming.amqp.AMQPUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import org.apache.spark.util.sketch.CountMinSketch
import io.radanalytics.equoid._
import org.apache.spark.sql.SparkSession

import org.apache.spark.util.sketch
import org.apache.spark.util.LongAccumulator
import scala.util.Random
import scala.util.Properties

import org.infinispan._
import org.infinispan.client.hotrod.RemoteCache
import org.infinispan.client.hotrod.RemoteCacheManager
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.impl.ConfigurationProperties

import scala.collection.immutable

object FileIntervalAccumulator {

  @volatile private var instance: LongAccumulator = null

  def getInstance(sc: SparkContext): LongAccumulator = {
    if (instance == null) {
      synchronized { 
        if (instance == null) {
          instance = sc.longAccumulator("FileIntervalCounter")
        }
      }
    }
    instance
  }
}

object FileDataHandler {

  private var master: String = "local[2]"
  private val appName: String = getClass().getSimpleName()

  private val batchIntervalSeconds: Int = 5
  private val checkpointDir: String = "/tmp/equoid-data-handler"

  def getProp(camelCaseName: String, defaultValue: String): String = {
    val snakeCaseName = camelCaseName.replaceAll("(.)(\\p{Upper})", "$1_$2").toUpperCase()
    Properties.envOrElse(snakeCaseName, Properties.scalaPropOrElse(snakeCaseName, defaultValue))
  }  
  
  def main(args: Array[String]): Unit = {

    master = "local[4]"

    val ssc = StreamingContext.getOrCreate(checkpointDir, createStreamingContext)
    
    ssc.start()
    ssc.awaitTerminationOrTimeout(batchIntervalSeconds * 1000 * 1000)
    ssc.stop()
  }

  def storeTopK(interval: Long, topk: Vector[(String, Int)], infinispanHost: String, infinispanPort: Int): Unit = {
    val builder: ConfigurationBuilder = new ConfigurationBuilder()
    builder.addServer().host(infinispanHost).port(infinispanPort)
    val cacheManager = new RemoteCacheManager(builder.build())
    val cache = cacheManager.getCache[String, String]()
    var topkstr: String = ""

    for ((key,v) <- topk) topkstr = topkstr + key + ":" + v.toString + ";" 
    cache.put(interval.toString, topkstr)
    cacheManager.stop()
  }

  def createStreamingContext(): StreamingContext = {
    val infinispanHost = getProp("jdgHost", "localhost")
    val infinispanPort = getProp("jdgPort", "11222").toInt
    val k = getProp("cmsK", "3").toInt
    val epsilon = getProp("cmsEpsilon", "0.01").toDouble
    val confidence = getProp("cmsConfidence", "0.9").toDouble    
    var globalTopK = TopK.empty[String](k, epsilon, confidence)
    var windowTopK = TopK.empty[String](k, epsilon, confidence)
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    
    val ssc = new StreamingContext(conf, Seconds(batchIntervalSeconds))
    ssc.checkpoint(checkpointDir)
   
    val receiveStream = ssc.textFileStream("/tmp/datafiles/")
     .transform( rdd => { 
        rdd.mapPartitions( rows => { 
          Iterator(rows.foldLeft(TopK.empty[String](k, epsilon, confidence))(_ + _ )) 
      })
    })
    .reduceByWindow(_ ++ _, Seconds(10), Seconds(10)) 
    .foreachRDD(rdd => {
      val intervalCounter = FileIntervalAccumulator.getInstance(rdd.sparkContext)
      val interval = intervalCounter.sum
      storeTopK(interval, rdd.first.topk, infinispanHost, infinispanPort)
      intervalCounter.add(1)
    })
    ssc
  }
}
