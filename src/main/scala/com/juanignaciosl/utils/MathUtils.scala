package com.juanignaciosl.utils

trait MathUtils {
  def percentile[T](values: Vector[T], percentiles: Seq[Double])
                   (implicit ordering: Ordering[T]): Seq[T] = {
    if (values.isEmpty) return Seq()

    val sorted = values.sorted
    percentiles.map { p =>
      val size = sorted.size
      sorted(Math.ceil(p * size).toInt - 1)
    }
  }

}
