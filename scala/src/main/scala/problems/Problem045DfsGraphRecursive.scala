package problems
object Problem045DfsGraphRecursive {
  def dfs(g: Map[Int,List[Int]], start:Int): List[Int] = {
    val seen = scala.collection.mutable.LinkedHashSet[Int]()
    def rec(u:Int): Unit = if (!seen(u)) { seen += u; g.getOrElse(u,Nil).foreach(rec) }
    rec(start); seen.toList
  }
}
