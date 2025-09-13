package problems
import scala.collection.mutable

object Problem106SlidingWindowMedian {
  def medianSlidingWindow(nums:Array[Int], k:Int): Array[Double] = {
    val res = new Array[Double](nums.length - k + 1)
    val window = mutable.ArrayBuffer[Int]()
    var i=0
    while (i<k) { window += nums(i); i+=1 }
    def medianSorted(buf: mutable.ArrayBuffer[Int]): Double = {
      val s = buf.sorted
      val m = k/2
      if (k%2==1) s(m).toDouble else (s(m-1) + s(m))/2.0
    }
    res(0) = medianSorted(window)
    var l=0; var r=k
    while (r < nums.length) {
      // remove nums[l]
      val idx = window.indexOf(nums[l])
      window.remove(idx)
      window += nums[r]
      res(l+1) = medianSorted(window)
      l+=1; r+=1
    }
    res
  }
}

