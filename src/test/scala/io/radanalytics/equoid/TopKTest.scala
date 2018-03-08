package io.radanalytics.equoid

import org.scalatest._

class TopKTest extends FlatSpec with Matchers {

  import org.apache.commons.math3.distribution.ZipfDistribution
  import org.apache.commons.math3.random.{ RandomGenerator, Well512a }

  implicit val intToString = (j: Int) => j.toString

  val testseed = 1313
  val testrng: RandomGenerator = new Well512a(testseed)

  val epsilonDefault = 0.01
  val confidenceDefault = 0.99

  val zneDefault = 100
  val zexpDefault = 1.5

  def zipfianSample(n: Int, zne: Int = zneDefault, zexp: Double = zexpDefault): Vector[Int] = {
    val zd = new ZipfDistribution(testrng, zne, zexp)
    Vector.fill(n) { zd.sample() }
  }

  def zipfianTopK[V](k: Int, n: Int,
      epsilon: Double = epsilonDefault,
      confidence: Double = confidenceDefault,
      zne: Int = zneDefault,
      zexp: Double = zexpDefault)(implicit toV: Int => V): TopK[V] = {
    val z = TopK.empty[V](k, epsilon = epsilon, confidence = confidence, seed = testseed)
    zipfianSample(n, zne, zexp).map(toV).foldLeft(z) { case (tk, v) => tk + v }
  }

  def checkSimilarity[V](tka: TopK[V], tkb: TopK[V]): Unit = {
    // expecting same tuning parameters
    tka.k should be(tkb.k)
    tka.epsilon should be(tkb.epsilon)
    tka.confidence should be(tkb.confidence)

    // expecting they each saw same number of samples
    val n = tka.cms.totalCount()
    tkb.cms.totalCount() should be(n)

    tka.topk.zip(tkb.topk).foreach { case ((va, na), (vb, nb)) =>
      // element rankings should be same
      va should be(vb)
      // counts should be similar with respect to the probabilistic confidence bounds of CMS
      val r = (n * tka.epsilon).toInt
      na should be (nb +- r)
    }
  }

  behavior of "TopK + (insert)"

  "the + operator" should "correctly approximate distribution of top k objects" in {
    val zd = new ZipfDistribution(testrng, zneDefault, zexpDefault)
    val tk = zipfianTopK[Int](5, 10000)
    val h = tk.top(5)
    val n = tk.cms.totalCount()
    (1 to 5).foreach { j =>
      val (v, f) = h(j - 1)
      v should be (j)
      val t = (n * zd.probability(j)).toInt
      val r = (n * tk.epsilon).toInt
      f should be (t +- r)
    }
  }

  "the + operator" should "be robust to data sampling" in {
    val tka = zipfianTopK[String](5, 10000)
    val tkb = zipfianTopK[String](5, 10000)
    checkSimilarity(tka, tkb)
  }

  behavior of "TopK ++ (monoid merge)"

  "the ++ operator" should "be (statistically) commutative" in {
    val tka = zipfianTopK[Int](5, 10000)
    val tkb = zipfianTopK[Int](5, 10000)

    checkSimilarity(tka ++ tkb, tkb ++ tka)
  }

  "the ++ operator" should "be (statistically) associative" in {
    val tka = zipfianTopK[Int](5, 10000)
    val tkb = zipfianTopK[Int](5, 10000)
    val tkc = zipfianTopK[Int](5, 10000)

    checkSimilarity((tka ++ tkb) ++ tkc, tka ++ (tkb ++ tkc))
  }

  behavior of "TopK empty (monoid identity)"

  "TopK empty" should "be monoid identity" in {
    val tka = zipfianTopK[String](5, 1000)
    val tke = TopK.empty[String](5,
      epsilon = epsilonDefault,
      confidence = confidenceDefault,
      seed = testseed)

    checkSimilarity(tka ++ tke, tke ++ tka)
  }

  behavior of "TopK immutability"

  "TopK +" should "not change its operands" in {
    val topK = zipfianTopK[Int](5, 100)
    // hashCode is not perfect for this purpose, but it's good enough
    val originalHashCode = topK.hashCode
    topK + 42 // we don't care about the result
    assert(originalHashCode == topK.hashCode)
  }

  "TopK ++" should "not change its operands" in {
    val t1 = zipfianTopK[Int](5, 100)
    val t2 = zipfianTopK[Int](5, 100)
    val originalHashCode1 = t1.hashCode
    val originalHashCode2 = t2.hashCode
    t1 ++ t2 // don't care about the result
    assert(originalHashCode1 == t1.hashCode)
    assert(originalHashCode2 == t2.hashCode)
  }
}
