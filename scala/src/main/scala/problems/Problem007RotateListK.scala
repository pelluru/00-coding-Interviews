package problems
object Problem007RotateListK {
  def rotateRight(a:Array[Int], k:Int): Array[Int] = {
    if (a==null || a.isEmpty) a
    else {
      val n=a.length; val kk=((k % n)+n)%n
      (a.takeRight(kk) ++ a.dropRight(kk))
    }
  }
}
