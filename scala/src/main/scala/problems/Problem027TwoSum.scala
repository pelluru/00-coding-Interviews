package problems

object Problem027TwoSum {
  def twoSum(a: Array[Int], target: Int): Array[Int] = {
    val m = scala.collection.mutable.Map[Int,Int]()
    for (i <- a.indices) {
      val need = target - a(i)
      if (m.contains(need)) return Array(m(need), i)
      m(a(i)) = i
    }
    null
  }
}
