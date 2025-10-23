
/* Insert Delete GetRandom O(1) — ArrayBuffer+HashMap */
import scala.util.Random
import scala.collection.mutable
object InsertDeleteGetRandomO1 {
  class RandomizedSet {
    val arr = new scala.collection.mutable.ArrayBuffer[Int]()
    val pos = mutable.HashMap[Int,Int]()
    val rnd = new Random()
    def insert(v:Int):Boolean = {
      if (pos.contains(v)) return false
      pos(v) = arr.length; arr += v; true
    }
    def remove(v:Int):Boolean = pos.get(v) match {
      case None => false
      case Some(i) =>
        val last = arr(arr.length-1)
        arr(i) = last; pos(last) = i
        arr.remove(arr.length-1); pos.remove(v); true
    }
    def getRandom():Int = arr(rnd.nextInt(arr.length))
  }
  def main(args:Array[String]):Unit = {
    val rs = new RandomizedSet
    println(rs.insert(1)); println(rs.remove(2)); println(rs.insert(2)); println(Set(1,2).contains(rs.getRandom()))
  }
}
