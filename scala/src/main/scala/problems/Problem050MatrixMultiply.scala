
package problems

object Problem050MatrixMultiply {
  def multiply(A:Array[Array[Int]], B:Array[Array[Int]]): Array[Array[Int]] = {
    val m=A.length; val n=A(0).length; val p=B(0).length
    val C = Array.ofDim[Int](m, p)
    var i=0; while (i<m) {
      var k=0; while (k<n) {
        if (A[i][k] != 0) {
          var j=0; while (j<p) { C[i][j] += A[i][k] * B[k][j]; j+=1 }
        }
        k+=1
      }
      i+=1
    }
    C
  }
}

