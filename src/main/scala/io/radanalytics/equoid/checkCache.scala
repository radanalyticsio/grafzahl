package io.radanalytics.equoid

import java.lang.Long

import io.radanalytics.equoid._

import scala.util.Random

import org.infinispan._
import org.infinispan.client.hotrod.RemoteCache
import org.infinispan.client.hotrod.RemoteCacheManager
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.impl.ConfigurationProperties

import scala.collection.immutable._

object checkCache {

  private var infinispanHost: String = "datagrid-hotrod"
  private var infinispanPort: Int = 11333
  private var key: String = ""
  private var iterations: Int = 1

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: checkCache <JDGHostname> <JDGPort> <key> <iterations>")
      System.exit(1)
    }
    
    infinispanHost = args(0)
    infinispanPort = args(1).toInt
    key = args(2)
    iterations = args(3).toInt

    val builder: ConfigurationBuilder = new ConfigurationBuilder()
    builder.addServer().host(infinispanHost).port(infinispanPort)
    val cacheManager = new RemoteCacheManager(builder.build())

    var cache = cacheManager.getCache[String, Integer]("daikon")

    var ret = 0
    var i: Int = 0 
    for (i <- 1 to iterations) {
      ret = cache.get(key)
      if (ret!=None) {
          ret = ret + 1
      }
      else {
        ret = 1
      }
      println("Key and ret: " + key + " " + ret)
      Thread.sleep(5000)
    }
    cacheManager.stop()
  }
 
}
