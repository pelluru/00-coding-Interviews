package problems
object Problem005SecondLargest {
  def secondLargest(a:Array[Int]): Option[Int] = {
    if (a==null || a.length<2) None
    else {
      var first: java.lang.Integer = null
      var second: java.lang.Integer = null
      for (n <- a) {
        if (first==null || n>first) { second=first; first=n }
        else if (n!=first && (second==null || n>second)) second=n
      }
      Option(second).map(_.intValue)
    }
  }
}
