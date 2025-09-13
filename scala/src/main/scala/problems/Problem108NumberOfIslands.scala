package problems
object Problem108NumberOfIslands {
  def numIslands(grid:Array[Array[Char]]): Int = {
    if (grid.isEmpty) return 0
    val R=grid.length; val C=grid(0).length
    var cnt=0
    def dfs(r:Int,c:Int): Unit = {
      if (r<0 || c<0 || r>=R || c>=C || grid[r][c] != '1') return
      grid[r][c] = '0'
      dfs(r+1,c); dfs(r-1,c); dfs(r,c+1); dfs(r,c-1)
    }
    var i=0; while (i<R) {
      var j=0; while (j<C) {
        if (grid[i][j]=='1') { cnt+=1; dfs(i,j) }
        j+=1
      }
      i+=1
    }
    cnt
  }
}

