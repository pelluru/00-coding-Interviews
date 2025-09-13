package problems
object Problem029LongestCommonPrefix {
  def lcp(a:Array[String]): String = {
    if (a==null || a.isEmpty) "" else a.reduce((x,y) => x.zip(y).takeWhile{case (c,d)=>c==d}.map(_._1).mkString)
  }
}
