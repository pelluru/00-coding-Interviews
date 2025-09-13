package problems


object Problem066FlattenDictKeys {
  def flatten(m: Map[String, Any], prefix:String=""): Map[String,Any] = {
    m.flatMap { case (k,v) =>
      val nk = if (prefix.isEmpty) k else s"$prefix.$k"
      v match {
        case mm: Map[_,_] => flatten(mm.asInstanceOf[Map[String,Any]], nk)
        case other => Map(nk -> other)
      }
    }
  }
}

