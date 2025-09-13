package problems
object Problem092DeserializeGraphAdjlist {
  def deserialize(s:String): Map[Int,List[Int]] = {
    if (s.trim.isEmpty) return Map.empty
    s.split(";").map { part =>
      val kv = part.split(":"); val k = kv(0).toInt
      val vs = if (kv.length==1 || kv(1).isEmpty) Nil else kv(1).split(",").toList.map(_.toInt)
      k -> vs
    }.toMap
  }
}
