package problems
import scala.collection.mutable

object Problem110CourseScheduleFeasible {
  def canFinish(numCourses:Int, prerequisites:Array[Array[Int]]): Boolean = {
    val g = Array.fill(numCourses)(mutable.ListBuffer[Int]())
    val indeg = Array.fill(numCourses)(0)
    for (p <- prerequisites) { g(p(1)).append(p(0)); indeg(p(0)) += 1 }
    val q = mutable.Queue[Int]()
    for (i <- 0 until numCourses if indeg(i)==0) q.enqueue(i)
    var visited=0
    while (q.nonEmpty) {
      val u = q.dequeue(); visited += 1
      for (v <- g(u)) { indeg(v) -= 1; if (indeg(v)==0) q.enqueue(v) }
    }
    visited == numCourses
  }
}

