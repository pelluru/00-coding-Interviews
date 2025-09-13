
package problems

object Problem051TransposeMatrix {
  def transpose(M:Array[Array[Int]]): Array[Array[Int]] = {
    val m=M.length; val n=M(0).length
    val T = Array.ofDim[Int](n,m)
    var i=0; while (i<m) { var j=0; while (j<n) { T(j)(i)=M(i)(j); j+=1 }; i+=1 }
    T
  }
}

