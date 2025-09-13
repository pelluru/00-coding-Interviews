
package problems

object Problem103HammingDistance {
  def hamming(a:Int, b:Int): Int = {
    var x = a ^ b; var c=0
    while (x!=0) { x &= (x-1); c += 1 }
    c
  }
}

