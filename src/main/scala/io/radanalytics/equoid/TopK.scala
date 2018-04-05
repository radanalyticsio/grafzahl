// type parameter V is type of object values being counted
// class parameters are 'val'; this class is immutable
//

package io.radanalytics.equoid

import org.apache.spark.util.sketch.CountMinSketch

class TopK[V] ( 
  val k: Int,
  val cms: CountMinSketch,
  val topk: Vector[(V, Int)],
  val seed: Int) extends Serializable {

  // mmmm, sugary...
  def confidence = cms.confidence()
  def epsilon = cms.relativeError()

  // update the TopK sketch w/ a new element 'v'
  def +(v: V): TopK[V] = {
    val ecms: CountMinSketch = CountMinSketch.create(epsilon, confidence, seed)
    val ucms: CountMinSketch = ecms.mergeInPlace(this.cms)
    ucms.add(v, 1)
    val f = ucms.estimateCount(v).toInt
    val utopk = {
      // search through the current top-k table.
      // If v is already in the table, its recorded frequency will be < f, due to the
      // way that CMS estimates object frequencies.
      // Stop if we hit a frequency >= f, or if we find value v already in the table.
      val j = topk.indexWhere { case (tv, tf) => (tf >= f) || (v == tv) }
      if (j == -1) {
        // v is not present, and f is > all current frequencies. Add to the end.
        topk :+ (v, f)
      } else {
        val (jv, jf) = topk(j)
        if (j == 0 && f <= jf && topk.length == k) {
          // (v,f) doesn't fall into the current top-k, so no change
          topk
        } else if (v == jv) {
          // value 'v' already exists in the top-k, so update its frequency
          if ((j < topk.length - 1) && (f > topk(j + 1)._2)) {
            // (v, f) needs to be reordered, just re-sort it
            topk.updated(j, (v, f)).sortBy(_._2)
          } else {
            topk.updated(j, (v, f))
          }
        } else {
          // v is new, and has a place in the top k, so insert it
          topk.patch(j, Vector((v, f)), 0)
        }
      }
    }.takeRight(k)
    new TopK[V](k, ucms, utopk, seed)
  }

  // combine two TopK sketches, monoidally
  def ++(that: TopK[V]): TopK[V] = {
    val ecms: CountMinSketch = CountMinSketch.create(epsilon, confidence, seed)
    val thatcms = ecms.mergeInPlace(that.cms) 
    val ucms = thatcms.mergeInPlace(this.cms)
    val utopk =
      (this.topk ++ that.topk)
        .map { case (v, _) => v }
        .distinct
        .map { v => (v, ucms.estimateCount(v).toInt) }
        .sortBy { case (_, f) => f }
        .takeRight(k)
    new TopK[V](k, ucms, utopk, seed)
  }

  def top(n: Int): Vector[(V, Int)] = this.topk.reverse.take(n)

  override def toString: String = "[" + top(k).foldLeft("")((a, b) => a + s", ${b._1} ~${b._2}").drop(2) + "]"
}

// eps, confidence, seed
object TopK {
  def epsilonDefault = 0.01
  def confidenceDefault = 0.99
  def seedDefault = 13 //scala.util.Random.nextInt()

  def empty[V](k: Int,
      epsilon: Double = epsilonDefault,
      confidence: Double = confidenceDefault,
      seed: Int = seedDefault) =
    new TopK[V](k, CountMinSketch.create(epsilon, confidence, seed), Vector.empty[(V, Int)], seed)
}
