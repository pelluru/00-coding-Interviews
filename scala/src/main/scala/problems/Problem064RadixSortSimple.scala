
package problems

object Problem064RadixSortSimple {
  def sort(a:Array[Int]): Array[Int] = {
    var max = 0; a.foreach(x => if (x>max) max=x)
    val n = a.length; val out = Array.ofDim[Int](n)
    var exp = 1
    while (max/exp > 0) {
      val cnt = Array.fill(10)(0)
      var i=0; while (i<n) { cnt((a(i)/exp)%10) += 1; i+=1 }
      var d=1; while (d<10) { cnt(d) += cnt(d-1); d+=1 }
      i=n-1; while (i>=0) { val dgt=(a(i)/exp)%10; cnt(dgt)-=1; out(cnt(dgt))=a(i); i-=1 }
      System.arraycopy(out, 0, a, 0, n)
      exp *= 10
    }
    a
  }
}

