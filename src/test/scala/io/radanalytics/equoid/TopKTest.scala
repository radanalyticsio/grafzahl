package io.radanalytics.equoid

import org.scalatest._

class TopKTest extends FlatSpec {
  
  def empty[T] = TopK.empty[T](k = 10, epsilon = 0.001, confidence = 0.999)
  def emptyi = empty[Int]
  def randomSeq(limit: Int) = Seq.fill(limit)(util.Random.nextInt)
  def randomSeqTopk(limit: Int) = randomSeq(limit).foldLeft(emptyi)(_+_)
  def helperTopk(start: Int, end: Int, step: Int = 1) =
    (start to end by step).foldLeft(emptyi)((x, y) => Seq.fill(y)(y).foldLeft(x)(_+_))

  "A TopK" should "not be null after creation" in {
    assert(TopK.empty[String](k = 10, epsilon = 6.0, confidence = 0.9) != null)
  }

  behavior of "TopK::+ (append)"

  "Empty TopK + element" should "contain just the element" in {
    val element = "Kpot"
    val resultingTopK = empty[String] + element
    assert(resultingTopK.cms.estimateCount(element) == 1)
  }

  "The order of addition" should "not make any difference" in {
    val topK1 = helperTopk(4, 22)
    val topK2 = helperTopk(22, 4, -1)
    assert(topK1.cms == topK2.cms)
    assert(topK1.topk == topK2.topk)
  }

  "The fmin" should "be correctly calculated" in {
    val topK1 = helperTopk(6, 20)
    val topK2 = helperTopk(1, 4)
    val topK3 = helperTopk(5, 13)
    val topK4 = helperTopk(5, 14)
    assert(topK1.fmin == 11)
    assert(topK2.fmin == 0)
    assert(topK3.fmin == 0)
    assert(topK4.fmin == 5)
  }

  "Adding the same element twice" should "increase the frequency" in {
    val element = "Kpot"
    val nothing = empty[String]
    val topK1 = nothing + element
    val topK2 = topK1 + element
    assert(nothing.cms.estimateCount(element) < topK1.cms.estimateCount(element))
    assert(topK1.cms.estimateCount(element) < topK2.cms.estimateCount(element))
  }

  "Adding the same element couple of times" should "return good enough results" in {
    val element = "Kpot"
    val result = List.fill(42)("Kpot").foldLeft(empty[String])(_+_)
    assert(Math.abs(result.cms.estimateCount(element) - 42) < 5)
  }

  "A TopK + element" should "return another TopK in which the estimated frequency for the element is higher" in {
    val randomSequence = randomSeq(1000)
    val randomElement = randomSequence(0)
    val topK = randomSequence.foldLeft(emptyi)(_+_)
    // add the same element 10 times
    val result = List.fill(10)(randomElement).foldLeft(topK)(_+_)
    assert(Math.abs(result.cms.estimateCount(randomElement) - topK.cms.estimateCount(randomElement) - 10 ) < 2)
  }

  "the +" should "not change its operands" in {
    val topK = randomSeqTopk(100)
    // hashCode is not perfect for this purpose, but it's good enough
    val originalHashCode = topK.cms.hashCode
    topK + 42 // we don't care about the result
    assert(originalHashCode == topK.cms.hashCode)
  }


  behavior of "TopK::++ (concat)"

  // t ++ empty == t
  "A TopK ++ empty TopK" should "do nothing" in {
    val topK = randomSeqTopk(10)
    val result1 = topK ++ emptyi
    val result2 = emptyi ++ topK
    assert(topK.cms == result1.cms)
    assert(result1.cms == result2.cms)
    assert(topK.topk == result1.topk)
    assert(result1.topk == result2.topk)
  }

  "++" should "be associative1" in {
    val a = helperTopk(1, 11)
    val b = helperTopk(12, 18)
    val c = helperTopk(19, 25)
    assert(((a ++ b) ++ c).cms == (a ++ (b ++ c)).cms)
    assert(((a ++ b) ++ c).topk == (a ++ (b ++ c)).topk)
  }

  "++" should "be associative2" in {
    val a = helperTopk(1, 11)
    val b = helperTopk(3, 7)
    val c = helperTopk(5, 19)
    assert(((a ++ b) ++ c).cms == (a ++ (b ++ c)).cms)
    assert(((a ++ b) ++ c).topk == (a ++ (b ++ c)).topk)
  }

  "++" should "be commutative1" in {
    val a = helperTopk(4, 11)
    val b = helperTopk(2, 13)
    assert((a ++ b).cms == (b ++ a).cms)
    assert((a ++ b).topk == (b ++ a).topk)
  }

  "++" should "be commutative2" in {
    val a = helperTopk(4, 11)
    val b = helperTopk(13, 19)
    assert((a ++ b).cms == (b ++ a).cms)
    assert((a ++ b).topk == (b ++ a).topk)
  }

  "(t1 ++ t2) + e == (t1 + e) ++ t2 == t1 ++ (t2 + e)" should "hold" in {
    val t1 = helperTopk(4, 11)
    val t2 = helperTopk(3, 16)
    val e = Int.MinValue
    assert(((t1 ++ t2) + e).cms == ((t1 + e) ++ t2).cms)
    assert((t1 ++ (t2 + e)).cms == ((t1 + e) ++ t2).cms)
    assert(((t1 ++ t2) + e).topk == ((t1 + e) ++ t2).topk)
    assert((t1 ++ (t2 + e)).topk == ((t1 + e) ++ t2).topk)
  }

  "the ++" should "not change its operands" in {
    val t1 = helperTopk(14, 30)
    val t2 = helperTopk(7, 21)
    val originalHashCode1 = t1.cms.hashCode
    val originalHashCode2 = t2.cms.hashCode
    t1 ++ t2 // don't care about the result
    assert(originalHashCode1 == t1.cms.hashCode)
    assert(originalHashCode2 == t2.cms.hashCode)
  }

  "the frequency of a common element" should "increase if it's contained in both topKs" in {
    val t1 = helperTopk(14, 30)
    val t2 = helperTopk(7, 21)
    val e = 42424242
    val frequent = Seq.fill(100)(e).foldLeft(emptyi)(_+_)

    val res1 = t1 ++ frequent
    val res2 = frequent ++ t2
    assert(res1.cms.estimateCount(e) < (res1 ++ res2).cms.estimateCount(e))
    assert(Math.abs((res1 ++ res2).cms.estimateCount(e) - res1.cms.estimateCount(e) - 100) < 5)
  }

  "The order of concatenation" should "not make any difference" in {
    val t1 = helperTopk(3, 22, 2)
    val t2 = helperTopk(4, 22, 2)
    val t3 = helperTopk(22, 3, -2)
    val t4 = helperTopk(21, 3, -2)
    assert((t1 ++ t2).cms == (t3 ++ t4).cms)
    assert((t1 ++ t2).topk == (t3 ++ t4).topk)
  }
  
  
  behavior of "Other"
  
  "Some real-world example" should "contain the top k elements" in {
    val geometricTopk = (1 to 7).flatMap(i => {
      val num = Math.pow(2, i).toInt
      List.fill(num)(s"el-$num")
    }).foldLeft(empty[String])(_+_)
    val noise1 = (1 to 15).foldLeft(empty[String])((x, y) => Seq.fill(y)(s"el-$y").foldLeft(x)(_+_))
    val noise2 = randomSeq(1000).map("el-" + _).foldLeft(empty[String])(_+_)
    
    val topk = noise1 ++ geometricTopk ++ noise2
    assert(topk.topk.contains("el-128"))
    assert(topk.topk.contains("el-64"))
    assert(topk.topk.contains("el-32"))
    assert(topk.topk.maxBy(_._2)._1 == "el-128")
  }

  def sanEmpty[T] = TopK.empty[T](k = 3, epsilon = 0.001, confidence = 0.999)
  
  "Geometrically distributed data" should "contain the top 3 elements" in {
    val geometricTopk = (1 to 7).flatMap(i => {
      val num = Math.pow(2, i).toInt
      List.fill(num)(s"el-$num")
    }).foldLeft(sanEmpty[String])(_+_)
    val noise1 = (1 to 15).foldLeft(sanEmpty[String])((x, y) => Seq.fill(y)(s"el-$y").foldLeft(x)(_+_))
    val noise2 = randomSeq(10000).map("el-" + _).foldLeft(sanEmpty[String])(_+_)
    
    val topk = noise1 ++ geometricTopk ++ noise2
    assert(topk.topk.contains("el-128"))
    assert(topk.topk.contains("el-64"))
    assert(topk.topk.contains("el-32"))
    assert(topk.topk.maxBy(_._2)._1 == "el-128")
    assert(topk.topk.minBy(_._2)._1 == "el-32")

  }

}


