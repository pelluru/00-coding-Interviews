package patterns
object TwoPointers {
  def twoSumSorted(a:Array[Int], t:Int): Array[Int] = {
    var l=0; var r=a.length-1
    while (l<r){
      val s=a(l)+a(r)
      if (s==t) return Array(l+1,r+1)
      else if (s<t) l+=1 else r-=1
    }
    Array()
  }
  def threeSum(nums:Array[Int]): List[List[Int]] = {
    scala.util.Sorting.quickSort(nums)
    val n=nums.length; val res=scala.collection.mutable.ArrayBuffer[List[Int]]()
    var i=0
    while (i<n-2){
      var l=i+1; var r=n-1
      while (l<r){
        val s=nums(i)+nums(l)+nums(r)
        if (s==0){ res += List(nums(i),nums(l),nums(r)); l+=1; r-=1
          while (l<r && nums(l)==nums(l-1)) l+=1
          while (l<r && nums(r)==nums(r+1)) r-=1
        } else if (s<0) l+=1 else r-=1
      }
      i+=1; while (i<n-2 && nums(i)==nums(i-1)) i+=1
    }
    res.toList
  }
  def removeDuplicatesSorted(a:Array[Int]): Int = {
    if (a.isEmpty) return 0
    var w=1; var r=1
    while (r<a.length){ if (a(r)!=a(w-1)) { a(w)=a(r); w+=1 }; r+=1 }
    w
  }
}
