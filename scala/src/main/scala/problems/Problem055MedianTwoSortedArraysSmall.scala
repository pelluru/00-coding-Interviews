
package problems

object Problem055MedianTwoSortedArraysSmall {
  def median(A:Array[Int], B:Array[Int]): Double = {
    val n = A.length + B.length
    var i=0; var j=0; var prev=0; var cur=0
    var k=0; while (k<=n/2) {
      prev=cur
      if (i<A.length && (j>=B.length || A(i) <= B(j))) { cur=A(i); i+=1 }
      else { cur=B(j); j+=1 }
      k+=1
    }
    if (n%2==1) cur.toDouble else (prev + cur) / 2.0
  }
}

