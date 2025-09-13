
package problems

object Problem063CountingSortSimple {
  def sort(a:Array[Int], maxVal:Int): Array[Int] = {
    val count = Array.fill(maxVal+1)(0)
    a.foreach(x => count(x) += 1)
    var idx=0; var v=0
    while (v<=maxVal) { while (count(v) > 0) { a(idx)=v; idx+=1; count(v)-=1 }; v+=1 }
    a
  }
}

