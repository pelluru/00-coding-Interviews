package patterns
object DP2D {
  def editDistance(a:String,b:String): Int = {
    val m=a.length; val n=b.length
    val dp = Array.ofDim[Int](m+1,n+1)
    for (i<-0 to m) dp(i)(0)=i
    for (j<-0 to n) dp(0)(j)=j
    for (i<-1 to m; j<-1 to n){
      if (a(i-1)==b(j-1)) dp(i)(j)=dp(i-1)(j-1)
      else dp(i)(j)=1+math.min(dp(i-1)(j-1), math.min(dp(i-1)(j), dp(i)(j-1)))
    }
    dp(m)(n)
  }
  def lcsLength(a:String,b:String): Int = {
    val m=a.length; val n=b.length
    val dp = Array.ofDim[Int](m+1,n+1)
    for (i<-1 to m; j<-1 to n){
      dp(i)(j) = if (a(i-1)==b(j-1)) dp(i-1)(j-1)+1 else math.max(dp(i-1)(j), dp(i)(j-1))
    }
    dp(m)(n)
  }
  def uniquePathsWithObstacles(grid:Array[Array[Int]]): Int = {
    val m=grid.length; val n=if (m==0) 0 else grid(0).length
    if (m==0 || n==0 || grid(0)(0)==1) return 0
    val dp = Array.fill(m,n)(0); dp(0)(0)=1
    for (i<-0 until m; j<-0 until n){
      if (grid(i)(j)==1) dp(i)(j)=0
      else {
        if (i>0) dp(i)(j)+=dp(i-1)(j)
        if (j>0) dp(i)(j)+=dp(i)(j-1)
      }
    }
    dp(m-1)(n-1)
  }
}
