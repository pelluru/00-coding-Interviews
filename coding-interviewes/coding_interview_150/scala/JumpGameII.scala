
/* Jump Game II — greedy windows. O(n) */
object JumpGameII {
  def jump(nums:Array[Int]):Int = {
    var jumps=0; var curEnd=0; var far=0
    for (i <- 0 until nums.length-1){
      far = Math.max(far, i + nums(i))
      if (i==curEnd){ jumps += 1; curEnd = far }
    }
    jumps
  }
  def main(args:Array[String]):Unit = {
    println(jump(Array(2,3,1,1,4)))
    println(jump(Array(2,3,0,1,4)))
  }
}
