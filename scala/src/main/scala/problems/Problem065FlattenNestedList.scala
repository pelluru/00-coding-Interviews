package problems


object Problem065FlattenNestedList {
  def flatten(lst: List[Any]): List[Any] = lst.flatMap {
    case l: List[_] => flatten(l.asInstanceOf[List[Any]])
    case x => List(x)
  }
}

