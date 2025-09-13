
package problems

object Problem054FindDuplicate {
  def findDuplicate(a:Array[Int]): Int = {
    var slow=a(0); var fast=a(0)
    do { slow=a(slow); fast=a(a(fast)) } while (slow!=fast)
    slow=a(0)
    while (slow!=fast) { slow=a(slow); fast=a(fast) }
    slow
  }
}

