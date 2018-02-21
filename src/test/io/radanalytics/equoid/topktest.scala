package io.radanalytics.equoid

import org.scalatest._

class TopKTest extends FlatSpec {

  "A TopK" should "not be null after creation" in {
    var globalTopK:TopK[String] = null
    val k:Int = 10
    val epsilon:Double = 6.0
    val confidence:Double = 0.9
    globalTopK = TopK.empty[String](k, epsilon, confidence)
    assert(globalTopK != null)
  }


}
