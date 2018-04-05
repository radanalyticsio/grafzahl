package io.radanalytics.equoid

import java.lang.Long

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.LongAccumulator
import org.infinispan.client.hotrod.RemoteCacheManager
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder

import scala.util.Properties

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
    val infinispanHost = getProp("JDG_HOST", "localhost")
    val infinispanPort = getProp("JDG_PORT", "11222").toInt
    val k = getProp("CMS_K", "3").toInt
    val epsilon = getProp("CMS_EPSILON", "0.01").toDouble
    val confidence = getProp("CMS_CONFIDENCE", "0.9").toDouble
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
