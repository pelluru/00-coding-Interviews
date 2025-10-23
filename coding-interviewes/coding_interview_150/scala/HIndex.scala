
/* H-Index — sort & scan. O(n log n) */
object HIndex {
  def hIndex(c:Array[Int]):Int = {
    scala.util.Sorting.quickSort(c)
    val n=c.length
    for (i <- 0 until n){
      val papers = n - i
      if (c(i) >= papers) return papers
    }
    0
  }
  def main(args:Array[String]):Unit = {
    println(hIndex(Array(3,0,6,1,5)))
  }
}
