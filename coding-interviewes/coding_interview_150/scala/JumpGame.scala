
/* Jump Game — greedy max reach. O(n) */
object JumpGame {
  def canJump(nums:Array[Int]):Boolean = {
    var reach=0
    for (i <- 0 until nums.length){
      if (i>reach) return false
      reach = Math.max(reach, i + nums(i))
    }
    true
  }
  def main(args:Array[String]):Unit = {
    println(canJump(Array(2,3,1,1,4)))
    println(canJump(Array(3,2,1,0,4)))
  }
}
