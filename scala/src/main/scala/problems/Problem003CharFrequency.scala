package problems

object Problem003CharFrequency {
  def charFrequency(s: String): Map[Char, Int] =
    Option(s).map(_.toList.groupBy(identity).view.mapValues(_.size).toMap).getOrElse(Map.empty)
}
