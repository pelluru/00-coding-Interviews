package problems
object Problem091SerializeGraphAdjlist {
  def serialize(g: Map[Int,List[Int]]): String =
    g.toList.sortBy(_._0).map{case (k,vs)=> s"$k:" + vs.sorted.mkString(",")}.mkString(";")
}
