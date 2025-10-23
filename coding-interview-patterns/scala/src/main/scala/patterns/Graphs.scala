package patterns
import scala.collection.mutable
object Graphs {
  def numIslands(grid:Array[Array[Char]]): Int = {
    if (grid.isEmpty) return 0
    val m=grid.length; val n=grid(0).length
    val seen = Array.fill(m,n)(false)
    val D = Array((1,0),(-1,0),(0,1),(0,-1))
    def bfs(i:Int,j:Int):Unit = {
      val q = mutable.Queue[(Int,Int)]((i,j))
      seen(i)(j)=true
      while (q.nonEmpty){
        val (x,y)=q.dequeue()
        for ((dx,dy) <- D){
          val nx=x+dx; val ny=y+dy
          if (0<=nx && nx<m && 0<=ny && ny<n && !seen(nx)(ny) && grid(nx)(ny)=='1'){
            seen(nx)(ny)=true; q.enqueue((nx,ny))
          }
        }
      }
    }
    var ans=0
    for (i<-0 until m; j<-0 until n){
      if (grid(i)(j)=='1' && !seen(i)(j)){ ans+=1; bfs(i,j) }
    }
    ans
  }
  def ladderLength(begin:String, end:String, wordList:Array[String]): Int = {
    if (!wordList.contains(end)) return 0
    val L=begin.length
    val buckets = mutable.Map[String, mutable.ArrayBuffer[String]]().withDefaultValue(mutable.ArrayBuffer[String]())
    for (w <- wordList) for (i<-0 until L) buckets(w.substring(0,i)+"*"+w.substring(i+1)).append(w)
    val q = mutable.Queue[(String,Int)]((begin,1))
    val seen = mutable.Set[String](begin)
    while (q.nonEmpty){
      val (w,d)=q.dequeue()
      if (w==end) return d
      for (i<-0 until L){
        val key=w.substring(0,i)+"*"+w.substring(i+1)
        val arr=buckets.getOrElse(key, mutable.ArrayBuffer[String]())
        for (nei <- arr) if (!seen(nei)){ seen.add(nei); q.enqueue((nei, d+1)) }
        buckets(key)=mutable.ArrayBuffer[String]()
      }
    }
    0
  }
  // Clone graph omitted for brevity in Scala sample.
}
