package io.radanalytics.equoid

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.LongAccumulator

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

  private val checkpointDir: String = "/tmp/equoid-data-handler"

  def getProp(camelCaseName: String, defaultValue: String): String = {
    val snakeCaseName = camelCaseName.replaceAll("(.)(\\p{Upper})", "$1_$2").toUpperCase()
    Properties.envOrElse(snakeCaseName, Properties.scalaPropOrElse(snakeCaseName, defaultValue))
  }  
  
  def main(args: Array[String]): Unit = {

    master = "local[4]"

    val ssc = StreamingContext.getOrCreate(checkpointDir, createStreamingContext)
    ssc.sparkContext.setLogLevel("ERROR")
    
    ssc.start()
    ssc.awaitTerminationOrTimeout(5 * 1000 * 1000)
    ssc.stop()
  }

  def createStreamingContext(): StreamingContext = {
    val infinispanHost = getProp("JDG_HOST", "localhost")
    val infinispanPort = getProp("JDG_PORT", "11222").toInt
    val k = getProp("CMS_K", "5").toInt
    // the values for epsilon and confidence are as follows to have results closer to exact counting rather then
    // loosing some information due to the sketch nature
    val epsilon = getProp("CMS_EPSILON", "0.001").toDouble
    val confidence = getProp("CMS_CONFIDENCE", "0.999").toDouble

    val conf = new SparkConf().setMaster(master).setAppName(appName)
    val windowSeconds = getProp("WINDOW_SECONDS", "50").toInt
    val slideSeconds = getProp("SLIDE_SECONDS", "4").toInt
    val batchSeconds = getProp("BATCH_SECONDS", "2").toInt
    conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")

    val ssc = new StreamingContext(conf, Seconds(batchSeconds))
    ssc.checkpoint(checkpointDir)

    val receiveStream: DStream[String] = ssc.textFileStream("/tmp/datafiles/")
    receiveStream.transform(rdd => {
      val result: RDD[TopK[String]] = rdd.mapPartitions(partition => {
        Iterator(partition.foldLeft(TopK.empty[String](k, epsilon, confidence))(_ + _))
      })
      result
    }).reduceByWindow((t1, t2) => {
      println(" left operand: " + t1)
      println("right operand: " + t2)
      val result: TopK[String] = t1 ++ t2
      println("       result: " + result)
      result
    }, Seconds(windowSeconds), Seconds(slideSeconds))
      .print()
    ssc
  }
}
