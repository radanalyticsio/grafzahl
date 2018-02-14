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

  private var infinispanHost: String = "localhost"
  private var infinispanPort: Int = 11333

  def main(args: Array[String]): Unit = {
    var key = args(0)
    val builder: ConfigurationBuilder = new ConfigurationBuilder()
    builder.addServer().host(infinispanHost).port(infinispanPort)
    val cacheManager = new RemoteCacheManager(builder.build())

    var cache = cacheManager.getCache[String, Integer]()

    var ret = cache.get(key)
    cache.put("Testing", 3)
    if (ret!=null) {
        ret = ret + 1
    }
    else {
      ret = 1
    }
    println("Key and ret: " + key + " " + ret)
    cacheManager.stop()
  }
 
}
