
package problems

object Problem053FindMissingNumber {
  def missing(a:Array[Int]): Int = {
    val n=a.length
    val expected = n*(n+1)/2
    expected - a.sum
  }
}

