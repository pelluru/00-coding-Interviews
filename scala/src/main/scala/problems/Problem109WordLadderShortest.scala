package problems
import scala.collection.mutable

object Problem109WordLadderShortest {
  def ladderLength(beginWord:String, endWord:String, wordList:List[String]): Int = {
    val dict = wordList.toSet
    if (!dict.contains(endWord)) return 0
    val q = mutable.Queue[(String,Int)]((beginWord,1))
    val seen = mutable.HashSet[String](beginWord)
    while (q.nonEmpty) {
      val (w,d) = q.dequeue()
      if (w == endWord) return d
      val arr = w.toCharArray
      for (i <- arr.indices) {
        val old = arr(i)
        var c='a'
        while (c <= 'z') {
          arr(i)=c
          val nw = new String(arr)
          if (dict.contains(nw) && !seen(nw)) { seen+=nw; q.enqueue((nw,d+1)) }
          c = (c.toInt + 1).toChar
        }
        arr(i)=old
      }
    }
    0
  }
}

