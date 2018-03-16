package io.radanalytics.equoid

import java.lang.Long

import io.radanalytics.equoid._

import scala.util.Random

import org.infinispan._
import org.infinispan.client.hotrod.RemoteCache
import org.infinispan.client.hotrod.RemoteCacheManager
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.configuration.Configuration
import org.infinispan.client.hotrod.impl.ConfigurationProperties

import scala.collection.immutable
import scala.util.Properties

object CheckCache {

  def getProp(camelCaseName: String, defaultValue: String): String = {
        val snakeCaseName = camelCaseName.replaceAll("(.)(\\p{Upper})", "$1_$2").toUpperCase()
            Properties.envOrElse(snakeCaseName, Properties.scalaPropOrElse(snakeCaseName, defaultValue))
              }  

  def main(args: Array[String]): Unit = {
    val infinispanHost = getProp("jdgHost", "datagrid-hotrod")
    val infinispanPort = getProp("jdgPort", "11333").toInt
    val iterations = getProp("ccIter", "15").toInt
    
    val builder: ConfigurationBuilder = new ConfigurationBuilder()
    builder.addServer().host(infinispanHost).port(infinispanPort)
    val cacheManager = new RemoteCacheManager(builder.build())

    var cache = cacheManager.getCache[String, String]()
    var i: Int = 0 
    var ret: String = "" 
    for (i <- 1 to iterations) {
      for (k <- cache.keySet.toArray) {
        ret = cache.get(k)
        println(k + ": " + ret)
      }
      Thread.sleep(10000)
    }
    cacheManager.stop()
  }
 
}
