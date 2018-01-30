// type parameter V is type of object values being counted
// class parameters are 'val'; this class is immutable
//

package io.radanalytics.equoid

import scala.util.Random
import org.apache.spark.util.sketch.CountMinSketch

import scala.collection.immutable

class TopK[V](
  val k: Int,
  val cms: CountMinSketch,
  val topk: scala.collection.immutable.Map[V, Int],
  val fmin: Int) {
  
  // update the TopK sketch w/ a new element 'v'
  def +(v: V): TopK[V] = {
    val ecms: CountMinSketch = CountMinSketch.create(k, k*5, 13)
    val ucms: CountMinSketch = ecms.mergeInPlace(this.cms)
    ucms.add(v, 1)
    val vf = ucms.estimateCount(v).toInt
    val (utopk, ufmin) = if (topk.size < k) {
      (topk + (v -> vf), math.min(vf, fmin))
    } else if (vf <= fmin) (topk, fmin) else {
      val del = topk.minBy { case (_, f) => f }
      ((topk - del._1) + ((v, vf)), topk.values.min)
    }
    new TopK[V](k, ucms, utopk, ufmin)
  }
  
  // combine two TopK sketches, monoidally
  def ++(that: TopK[V]): TopK[V] = {
    val ucms = that.cms.mergeInPlace(this.cms)
    val vu = this.topk.keys.toSet ++ that.topk.keys.toSet
    val (utopk, ufmin) = vu.foldLeft((Map.empty[V, Int], 0)) { case (v, (tk, fm)) =>
      val vf = ucms.estimateCount(v).toInt
      if (tk.size < k) {
        (tk + (v -> vf), tk.min(vf, fm))
      } else if (vf <= fm) (tk, fm) else {
        val del = map.minBy { case (_, f) => f }
        ((tk - del._1) + (v, vf), tk.values.min)
      }
    }
    new TopK[V](k, ucms, utopk, ufmin)
  }
}

object TopK {
  def empty[V](k: Int) = new TopK[V](k, CountMinSketch.create(k, k*5, 13), immutable.Map.empty[V, Int], 0)
}
