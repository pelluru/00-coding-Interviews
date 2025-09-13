package problems
object Problem026AnagramGroups {
  def group(words:Array[String]): List[List[String]] =
    words.groupBy(w => w.sorted).values.map(_.toList).toList
}
