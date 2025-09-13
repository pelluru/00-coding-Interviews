package problems


object Problem062BucketSortSimple {
  def sort(a:Array[Double]): Array[Double] = {
    val n = a.length
    val buckets = Array.fill(n)(List[Double]())
    for (x <- a) {
      val idx = math.min((x * n).toInt, n-1).max(0)
      buckets(idx) = (x :: buckets(idx)).sorted
    }
    buckets.flatten
  }
}

