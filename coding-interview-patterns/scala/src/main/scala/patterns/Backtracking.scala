package patterns
object Backtracking {
  def permutations(nums:Array[Int]): List[List[Int]] = {
    val used=Array.fill(nums.length)(false); val cur=scala.collection.mutable.ArrayBuffer[Int](); val res=scala.collection.mutable.ArrayBuffer[List[Int]]()
    def dfs():Unit = { if (cur.length==nums.length) res+=cur.toList else for (i<-nums.indices) if (!used[i]){ used[i]=true; cur+=nums(i); dfs(); cur.remove(cur.length-1); used[i]=false } }
    dfs(); res.toList
  }
  def combinationSum(cands:Array[Int], target:Int): List[List[Int]] = {
    scala.util.Sorting.quickSort(cands); val res=scala.collection.mutable.ArrayBuffer[List[Int]](); val cur=scala.collection.mutable.ArrayBuffer[Int]()
    def dfs(i:Int, rem:Int):Unit = {
      if (rem==0) res+=cur.toList
      else if (i<cands.length && rem>0){ cur+=cands(i); dfs(i, rem-cands(i)); cur.remove(cur.length-1); dfs(i+1, rem) }
    }
    dfs(0, target); res.toList
  }
  def subsetsWithDup(nums:Array[Int]): List[List[Int]] = {
    scala.util.Sorting.quickSort(nums); val res=scala.collection.mutable.ArrayBuffer[List[Int]](); val cur=scala.collection.mutable.ArrayBuffer[Int]()
    def dfs(i:Int):Unit = {
      res+=cur.toList
      var j=i
      while (j<nums.length){
        cur+=nums(j); dfs(j+1); cur.remove(cur.length-1)
        j+=1; while (j<nums.length && nums(j)==nums(j-1)) j+=1
      }
    }
    dfs(0); res.toList
  }
}
