package problems


object Problem068GroupByKey {
  def groupByKey[K,V](pairs: List[(K,V)]): Map[K,List[V]] =
    pairs.groupBy(_._1).view.mapValues(_.map(_._2)).toMap
}

