
package problems

object Problem102CountSetBits {
  def countBits(x:Int): Int = {
    var n=x; var c=0
    while (n!=0) { n &= (n-1); c += 1 }
    c
  }
}

