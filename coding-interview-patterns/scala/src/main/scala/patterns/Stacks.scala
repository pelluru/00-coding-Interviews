package patterns
object Stacks {
  def nextGreater(nums:Array[Int]): Array[Int] = {
    val st = scala.collection.mutable.Stack[Int]()
    val res = Array.fill(nums.length)(-1)
    for (i<-nums.indices){
      while (st.nonEmpty && nums(st.top) < nums(i)) res(st.pop()) = nums(i)
      st.push(i)
    }
    res
  }
  def dailyTemperatures(T:Array[Int]): Array[Int] = {
    val st = scala.collection.mutable.Stack[Int]()
    val res = Array.fill(T.length)(0)
    for (i<-T.indices){
      while (st.nonEmpty && T(st.top) < T(i)){
        val j=st.pop(); res(j)=i-j
      }
      st.push(i)
    }
    res
  }
  def largestRectangle(h:Array[Int]): Int = {
    val st=scala.collection.mutable.Stack[Int]()
    val a=h :+ 0
    var ans=0
    for (i<-a.indices){
      while (st.nonEmpty && a(st.top) > a(i)){
        val H=a(st.pop()); val L= if (st.isEmpty) -1 else st.top
        ans = ans.max(H * (i - L - 1))
      }
      st.push(i)
    }
    ans
  }
}
