
package problems

object Problem049RotateMatrix90 {
  def rotate(m:Array[Array[Int]]): Unit = {
    val n = m.length
    var i=0; while (i<n) {
      var j=i; while (j<n) {
        val t = m(i)(j); m(i)(j)=m(j)(i); m(j)(i)=t
        j+=1
      }
      i+=1
    }
    var r=0; while (r<n) {
      var l=0; var h=n-1
      while (l<h) { val t=m(r)(l); m(r)(l)=m(r)(h); m(r)(h)=t; l+=1; h-=1 }
      r+=1
    }
  }
}

