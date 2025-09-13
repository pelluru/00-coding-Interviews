package problems
object Problem080JsonSerializeCustom {
  def toJson(m: Map[String,Any]): String = {
    def esc(s:String) = s.replace("\\","\\\\").replace(""","\\"")
    def render(v:Any): String = v match {
      case s:String => """ + esc(s) + """
      case b:Boolean => b.toString
      case n:Int => n.toString
      case n:Long => n.toString
      case d:Double => if (d.isNaN) ""NaN"" else d.toString
      case l:List[_] => l.map(render).mkString("[",",","]")
      case m:Map[_,_] => toJson(m.asInstanceOf[Map[String,Any]])
      case null => "null"
      case other => """ + esc(other.toString) + """
    }
    m.map{case(k,v)=> """ + esc(k) + "":" + render(v)}.mkString("{",",","}")
  }
}
