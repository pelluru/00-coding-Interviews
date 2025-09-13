
package problems
import scala.collection.mutable.ArrayBuffer

object Problem052SpiralMatrix {
  def spiral(m:Array[Array[Int]]): List[Int] = {
    val res = ArrayBuffer[Int]()
    if (m.isEmpty) return res.toList
    var top=0; var bot=m.length-1; var left=0; var right=m(0).length-1
    while (top<=bot && left<=right) {
      var j=left; while(j<=right){ res += m(top)(j); j+=1 }; top+=1
      var i=top; while(i<=bot){ res += m(i)(right); i+=1 }; right-=1
      if (top<=bot) { j=right; while(j>=left){ res += m(bot)(j); j-=1 }; bot-=1 }
      if (left<=right) { i=bot; while(i>=top){ res += m(i)(left); i-=1 }; left+=1 }
    }
    res.toList
  }
}

