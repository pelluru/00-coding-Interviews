package problems
object Problem006RemoveDuplicates {
  def dedup(a:Array[Int]): Array[Int] = if (a==null) null else a.distinct
}
