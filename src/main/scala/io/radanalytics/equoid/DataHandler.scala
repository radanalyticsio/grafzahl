package io.radanalytics.equoid

import org.apache.qpid.proton.amqp.messaging.AmqpValue
import org.apache.qpid.proton.message.Message
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.amqp.AMQPUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.infinispan.client.hotrod.RemoteCacheManager
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder

object DataHandler {

  private val checkpointDir: String = "/tmp/equoid-data-handler"

  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getOrCreate(checkpointDir, createStreamingContext)
    ssc.sparkContext.setLogLevel("ERROR")
    
    ssc.start()
    ssc.awaitTerminationOrTimeout(5 * 1000 * 1000)
    ssc.stop()
  }

  def messageConverter(message: Message, opMode: String): Option[String] = {

    message.getBody match {
      case body: AmqpValue => {
        val itemID: String = body.getValue.asInstanceOf[String]
        val primaryVal: String = itemID.split(",")(0)
        opMode match {
          case "linear" => Some(primaryVal)
          case "single" => Some(primaryVal)
          case "dual" => Some(itemID)
          case x => { println(s"unexpected opMode"); None }
        }
      }
      case x => { println(s"unexpected type ${x.getClass.getName}"); None }
    }
  }

  def storeTopK(interval: String, topk: Vector[(String, Int)], infinispanHost: String, infinispanPort: Int): Unit = {
    val builder: ConfigurationBuilder = new ConfigurationBuilder()
    builder.addServer().host(infinispanHost).port(infinispanPort)
    val cacheManager = new RemoteCacheManager(builder.build())
    val cache = cacheManager.getCache[String, String]()
    var topkstr: String = ""

    for ((key,v) <- topk) topkstr = topkstr + key + ":" + v.toString + ";"
    println(s"\nStoring top-k:\n$topk\n..for the last $interval seconds into JDG.")
    cache.put(interval + " Seconds", topkstr)
    cacheManager.stop()
  }

  def createStreamingContext(): StreamingContext = {
    val amqpHost = getProp("AMQP_HOST", "broker-amq-amqp")
    val amqpPort = getProp("AMQP_PORT", "5672").toInt
    val username = Option(getProp("AMQP_USERNAME", "daikon"))
    val password = Option(getProp("AMQP_PASSWORD", "daikon"))
    val address = getProp("QUEUE_NAME", "recordq")
    val infinispanHost = getProp("JDG_HOST", "datagrid-hotrod")
    val infinispanPort = getProp("JDG_PORT", "11222").toInt
    val k = getProp("CMS_K", "3").toInt
    val epsilon = getProp("CMS_EPSILON", "0.01").toDouble
    val confidence = getProp("CMS_CONFIDENCE", "0.9").toDouble
    val windowSeconds = getProp("WINDOW_SECONDS", "30").toInt
    val slideSeconds = getProp("SLIDE_SECONDS", "30").toInt
    val batchSeconds = getProp("SLIDE_SECONDS", "30").toInt
    val opMode = getProp("OP_MODE", "single")
    // store something in the JDG for this interval so that we can give something quickly to the user
    storeTopK(windowSeconds.toString, Vector(("nothing", 0)), infinispanHost, infinispanPort)

    val conf = new SparkConf()
    conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")

    val ssc = new StreamingContext(conf, Seconds(batchSeconds))
    ssc.checkpoint(checkpointDir)

    val receiveStream = AMQPUtils.createStream(ssc, amqpHost, amqpPort, username, password, address, messageConverter (_,opMode), StorageLevel.MEMORY_ONLY)
      .transform( rdd => {
        rdd.mapPartitions( rows => {
          Iterator(rows.foldLeft(TopK.empty[String](k, epsilon, confidence))(_ + _))
        })
      })
      .reduceByWindow(_ ++ _, Seconds(windowSeconds), Seconds(slideSeconds))
      .foreachRDD(rdd => {
        storeTopK(windowSeconds.toString, rdd.first.topk, infinispanHost, infinispanPort)
    })
    ssc
  }
}
