package io.radanalytics.equoid

import org.scalatest._

class TopKTest extends FlatSpec {

  "A TopK" should "not be null after creation" in {
    assert(TopK.empty[String](k = 10, epsilon = 6.0, confidence = 0.9) != null)
  }
  
  behavior of "TopK::+ (append)"
  
  "Empty TopK + element" should "contain just the element" in {
    val topK = TopK.empty[String](k = 10, epsilon = 6.0, confidence = 0.9)
    val element = "Kpot"
    val resultingTopK = topK + element
    assert(resultingTopK.cms.estimateCount(element) == 1)
  }
  
  "Adding the same element twice" should "increase the frequency" in {
    val emptyTopK = TopK.empty[String](k = 10, epsilon = 6.0, confidence = 0.9)
    val element = "Kpot"
    val topK1 = emptyTopK + element
    val topK2 = topK1 + element
    assert(emptyTopK.cms.estimateCount(element) < topK1.cms.estimateCount(element))
    assert(topK1.cms.estimateCount(element) < topK2.cms.estimateCount(element))
  }
  
  "Adding the same element couple of times" should "return exact results" in {
    // because there are no hash collisions yet
    val emptyTopK = TopK.empty[String](k = 10, epsilon = 6.0, confidence = 0.9)
    val element = "Kpot"
    val result = List.fill(42)("Kpot").foldLeft(emptyTopK)(_+_)
    assert(result.cms.estimateCount(element) == 42)
  }
  
  "A TopK + element" should "return another TopK in which the estimated frequency for the element is higher" in {
    val emptyTopK = TopK.empty[Int](k = 10, epsilon = 6.0, confidence = 0.9)
    val randomSeq = Seq.fill(1000)(util.Random.nextInt)
    val randomElement = randomSeq(0)
    val topK = randomSeq.foldLeft(emptyTopK)(_+_)
    // add the same element 10 times
    val result = List.fill(10)(randomElement).foldLeft(topK)(_+_)
    assert(result.cms.estimateCount(randomElement) - topK.cms.estimateCount(randomElement) == 10)
  }
  
  behavior of "TopK::++ (concat)"
  // tbd
  
}
