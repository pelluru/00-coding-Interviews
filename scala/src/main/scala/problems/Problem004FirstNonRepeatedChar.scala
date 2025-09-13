package problems
object Problem004FirstNonRepeatedChar {
  def firstNonRepeated(s:String): Option[Char] = {
    if (s==null) None
    else {
      val counts = s.foldLeft(scala.collection.mutable.LinkedHashMap.empty[Char,Int])((m,c) => { m(c)=m.getOrElse(c,0)+1; m })
      counts.collectFirst{ case (ch,c) if c==1 => ch }
    }
  }
}
