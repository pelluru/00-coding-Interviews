package problems
import scala.collection.mutable

object Problem034WordBreak {
  def wordBreak(s:String, dict:Set[String]): Boolean = {
    val dp = Array.fill(s.length+1)(false)
    dp(0)=true
    for (i <- 1 to s.length) {
      var ok=false
      var j=0
      while (j<i && !ok) {
        if (dp(j) && dict.contains(s.substring(j,i))) ok=true
        j+=1
      }
      dp(i)=ok
    }
    dp(s.length)
  }
}
