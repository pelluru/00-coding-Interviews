
package problems

object Problem056SearchInsertPosition {
  def searchInsert(a:Array[Int], target:Int): Int = {
    var lo=0; var hi=a.length-1
    while (lo<=hi) {
      val mid=(lo+hi)>>>1
      if (a(mid)==target) return mid
      else if (a(mid)<target) lo=mid+1
      else hi=mid-1
    }
    lo
  }
}

