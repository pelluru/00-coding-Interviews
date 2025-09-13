package problems


object Problem069MapReduceWordCount {
  def wordCount(lines: List[String]): Map[String, Int] =
    lines.flatMap(_.toLowerCase.split("\W+").filter(_.nonEmpty))
         .groupBy(identity).view.mapValues(_.size).toMap
}

