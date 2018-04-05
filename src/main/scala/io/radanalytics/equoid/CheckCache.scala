package io.radanalytics.equoid

import org.infinispan.client.hotrod.RemoteCacheManager
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder

object CheckCache {

  def main(args: Array[String]): Unit = {
    val infinispanHost = getProp("JDG_HOST", "datagrid-hotrod")
    val infinispanPort = getProp("JDG_PORT", "11222").toInt
    val iterations = getProp("CC_ITER", "15").toInt
    
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
