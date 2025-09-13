package problems
object Problem048SudokuSolver {
  def solve(board:Array[Array[Char]]): Boolean = {
    def ok(r:Int,c:Int,ch:Char): Boolean = {
      for(i<-0 until 9) {
        if(board(r)(i)==ch || board(i)(c)==ch) return false
      }
      val br=(r/3)*3; val bc=(c/3)*3
      for(i<-0 until 3; j<-0 until 3) if(board(br+i)(bc+j)==ch) return false
      true
    }
    def backtrack(pos:Int): Boolean = {
      if(pos==81) return true
      val r=pos/9; val c=pos%9
      if(board(r)(c)!='.') return backtrack(pos+1)
      for(ch <- "123456789") {
        if(ok(r,c,ch)) { board(r)(c)=ch; if(backtrack(pos+1)) return true; board(r)(c)='.' }
      }
      false
    }
    backtrack(0)
  }
}
