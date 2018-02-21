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

  "The order of addition" should "not make any difference" in {
    val emptyTopK1 = TopK.empty[Int](k = 10, epsilon = 6.0, confidence = 0.9)
    val emptyTopK2 = TopK.empty[Int](k = 10, epsilon = 6.0, confidence = 0.9)
    val randomSeq = Seq.fill(100)(util.Random.nextInt)
    val topK1 = randomSeq.foldLeft(emptyTopK1)(_+_)
    val topK2 = randomSeq.reverse.foldLeft(emptyTopK1)(_+_)
    assert(topK1.cms == topK2.cms)
    // this currently fails, but shouldn't
    // assert(topK1.topk == topK2.topk)
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

  "the +" should "not change its operands" in {
    val topK = Seq.fill(100)(util.Random.nextInt).foldLeft(TopK.empty[Int](10, 6.0, 0.9))(_+_)
    // hashCode is not perfect for this purpose, but it's good enough
    val originalHashCode = topK.cms.hashCode
    topK + 42 // we don't care about the result
    assert(originalHashCode == topK.cms.hashCode)
  }


  behavior of "TopK::++ (concat)"

  // t ++ empty == t
  "A TopK ++ empty TopK" should "do nothing" in {
    val emptyTopK = TopK.empty[Int](k = 10, epsilon = 6.0, confidence = 0.9)
    val topK = Seq.fill(10)(util.Random.nextInt).foldLeft(emptyTopK)(_+_)
    val result1 = topK ++ emptyTopK
    val result2 = emptyTopK ++ topK
    assert(topK.cms == result1.cms)
    assert(result1.cms == result2.cms)
    // this currently fails, but shouldn't
    // assert(topK.topk == result1.topk)
    // assert(result1.topk == result2.topk)
  }

  "++" should "be associative" in {
    val a = Seq.fill(10)(util.Random.nextInt).foldLeft(TopK.empty[Int](10, 6.0, 0.9))(_+_)
    val b = Seq.fill(20)(util.Random.nextInt).foldLeft(TopK.empty[Int](10, 6.0, 0.9))(_+_)
    val c = Seq.fill(30)(util.Random.nextInt).foldLeft(TopK.empty[Int](10, 6.0, 0.9))(_+_)
    assert(((a ++ b) ++ c).cms == (a ++ (b ++ c)).cms)
    // this currently fails, but shouldn't
    // assert(((a ++ b) ++ c).topk == (a ++ (b ++ c)).topk)
  }

  "++" should "be commutative" in {
    val a = Seq.fill(10)(util.Random.nextInt).foldLeft(TopK.empty[Int](10, 6.0, 0.9))(_+_)
    val b = Seq.fill(20)(util.Random.nextInt).foldLeft(TopK.empty[Int](10, 6.0, 0.9))(_+_)
    assert((a ++ b).cms == (b ++ a).cms)
    // todo: delete this comment
    // this currently passes, which is strange actually wrt the above failing :)
    assert((a ++ b).topk == (b ++ a).topk)
  }

  "(t1 ++ t2) + e == (t1 + e) ++ t2 == t1 ++ (t2 + e)" should "hold" in {
    val t1 = Seq.fill(10)(util.Random.nextInt).foldLeft(TopK.empty[Int](10, 6.0, 0.9))(_+_)
    val t2 = Seq.fill(20)(util.Random.nextInt).foldLeft(TopK.empty[Int](10, 6.0, 0.9))(_+_)
    val e = Int.MinValue
    assert(((t1 ++ t2) + e).cms == ((t1 + e) ++ t2).cms)
    assert((t1 ++ (t2 + e)).cms == ((t1 + e) ++ t2).cms)
    // this currently fails, but shouldn't
    // assert(((t1 ++ t2) + e).topk == ((t1 + e) ++ t2).topk)
    // assert((t1 ++ (t2 + e)).topk == ((t1 + e) ++ t2).topk)
  }

  "the ++" should "not change its operands" in {
    val t1 = Seq.fill(10000)(util.Random.nextInt).foldLeft(TopK.empty[Int](10, 6.0, 0.9))(_+_)
    val t2 = Seq.fill(10000)(util.Random.nextInt).foldLeft(TopK.empty[Int](10, 6.0, 0.9))(_+_)
    val originalHashCode1 = t1.cms.hashCode
    val originalHashCode2 = t2.cms.hashCode
    t1 ++ t2 // don't care about the result
    assert(originalHashCode1 == t1.cms.hashCode)
    assert(originalHashCode2 == t2.cms.hashCode)
  }

  "the frequency of a common element" should "increase if it's contained in both topKs" in {
    val t1 = Seq.fill(10000)(util.Random.nextInt).foldLeft(TopK.empty[Int](100, 6.0, 0.9))(_+_)
    val t2 = Seq.fill(10000)(util.Random.nextInt).foldLeft(TopK.empty[Int](100, 6.0, 0.9))(_+_)
    val e = 42424242
    val frequent = Seq.fill(100)(e).foldLeft(TopK.empty[Int](10, 6.0, 0.9))(_+_)

    val res1 = t1 ++ frequent
    val res2 = frequent ++ t2
    assert(res1.cms.estimateCount(e) < (res1 ++ res2).cms.estimateCount(e))

    // todo: following lines are fishy
    // println(res1.cms.estimateCount(e)) // prints 10100
    // println(res2.cms.estimateCount(e)) // prints 10100
    // println((res1 ++ res2).cms.estimateCount(e)) // prints 20200
    // assert((res1 ++ res2).cms.estimateCount(e) - res1.cms.estimateCount(e) == 100)
  }

}
