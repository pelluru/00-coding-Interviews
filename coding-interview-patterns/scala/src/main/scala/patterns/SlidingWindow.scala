package patterns
import scala.collection.mutable
object SlidingWindow {
  def minWindow(s:String, t:String): String = {
    if (s.isEmpty || t.isEmpty) return ""
    val need = t.groupBy(identity).view.mapValues(_.length).toMap
    val have = mutable.Map[Char,Int]().withDefaultValue(0)
    var req=need.size; var formed=0
    var l=0; var best=(Int.MaxValue,0,0)
    for (r <- s.indices){
      val ch=s(r); have(ch)=have(ch)+1
      if (need.contains(ch) && have(ch)==need(ch)) formed+=1
      while (formed==req){
        if (r-l+1 < best._1) best=(r-l+1,l,r)
        val c=s(l); have(c)=have(c)-1
        if (need.contains(c) && have(c)<need(c)) formed-=1
        l+=1
      }
    }
    if (best._1==Int.MaxValue) "" else s.substring(best._2, best._3+1)
  }
  def longestSubstringNoRepeat(s:String): Int = {
    val last = mutable.Map[Char,Int]()
    var l=0; var best=0
    for (r <- s.indices){
      val ch=s(r)
      if (last.contains(ch) && last(ch)>=l) l=last(ch)+1
      last(ch)=r; best=best.max(r-l+1)
    }
    best
  }
  def maxSlidingWindow(nums:Array[Int], k:Int): Array[Int] = {
    val q = scala.collection.mutable.ArrayDeque[Int]()
    val out = scala.collection.mutable.ArrayBuffer[Int]()
    for (i <- nums.indices){
      while (q.nonEmpty && nums(q.last) <= nums(i)) q.removeLast()
      q.append(i)
      if (q.head <= i - k) q.removeHead()
      if (i >= k-1) out.append(nums(q.head))
    }
    out.toArray
  }
}
