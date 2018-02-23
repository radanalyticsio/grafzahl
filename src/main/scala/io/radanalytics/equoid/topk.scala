// type parameter V is type of object values being counted
// class parameters are 'val'; this class is immutable
//

package io.radanalytics.equoid

import org.apache.spark.util.sketch.CountMinSketch

import scala.collection.immutable

class TopK[V] ( 
  val k: Int,
  val cms: CountMinSketch,
  val topk: immutable.Map[V, Int],
  val fmin: Int,
  val epsilon: Double,
  val confidence: Double)  extends Serializable {

  // update the TopK sketch w/ a new element 'v'
  def +(v: V): TopK[V] = {
    val ecms: CountMinSketch = CountMinSketch.create(epsilon, confidence, 13)
    val ucms: CountMinSketch = ecms.mergeInPlace(this.cms)
    ucms.add(v, 1)
    val vf = ucms.estimateCount(v).toInt
    val (utopk, ufmin) = if (topk.size < k || topk.contains(v)) {
      (topk + (v -> vf), vf)
    } else if (vf <= fmin) (topk, fmin) else {
      val del = topk.minBy { case (_, f) => f }
      ((topk - del._1) + ((v, vf)), topk.values.min)
    }
    new TopK[V](k, ucms, utopk, ufmin, epsilon, confidence)
  }
  
  // combine two TopK sketches, monoidally
  def ++(that: TopK[V]): TopK[V] = {
    val ecms: CountMinSketch = CountMinSketch.create(epsilon, confidence, 13)
    val thatcms = ecms.mergeInPlace(that.cms) 
    val ucms = thatcms.mergeInPlace(this.cms)

    val utopk: immutable.Map[V, Int] = (this.topk.keys.toSet ++ that.topk.keys.toSet).map(key => {
      (key -> ucms.estimateCount(key).toInt)
    }).toVector.sortBy(- _._2).take(k).toMap
    val ufmin = utopk.values.min
    new TopK[V](k, ucms, utopk, ufmin, epsilon, confidence)
  }

  def top(n: Int): immutable.SortedMap[V, Int] = throw new NotImplementedError

  override def toString: String = throw new NotImplementedError
}

// eps, confidence, seed
object TopK {
  def empty[V](k: Int, epsilon: Double, confidence: Double) = new TopK[V](k, CountMinSketch.create(epsilon, confidence, 13), immutable.Map.empty[V, Int], 0, epsilon, confidence)
}
