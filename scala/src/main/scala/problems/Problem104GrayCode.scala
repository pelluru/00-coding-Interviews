
package problems
import scala.collection.mutable.ArrayBuffer

object Problem104GrayCode {
  def grayCode(n:Int): List[Int] = {
    val res = ArrayBuffer[Int](0)
    var i=0; while (i<n) {
      val add = 1<<i
      var j=res.size-1; while (j>=0) { res += (res(j) + add); j-=1 }
      i+=1
    }
    res.toList
  }
}

