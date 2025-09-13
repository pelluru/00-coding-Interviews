package problems
object Problem089GraphCycleDetection {
  def hasCycle(n:Int, edges:Array[Array[Int]]): Boolean = {
    val g = Array.fill(n)(List[Int]())
    for (e <- edges) g(e(0)) = e(1) :: g(e(0))
    val color = Array.fill(n)(0) // 0=unseen,1=visiting,2=done
    def dfs(u:Int): Boolean = {
      color(u)=1
      for (v <- g(u)) {
        if (color(v)==1) return true
        if (color(v)==0 && dfs(v)) return true
      }
      color(u)=2; false
    }
    (0 until n).exists(i => color(i)==0 && dfs(i))
  }
}
