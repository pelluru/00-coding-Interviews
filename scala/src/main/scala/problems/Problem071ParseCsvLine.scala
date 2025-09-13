package problems


object Problem071ParseCsvLine {
  def parse(line:String): List[String] = {
    val res = scala.collection.mutable.ListBuffer[String]()
    val sb = new StringBuilder
    var i=0; var inQ=false
    while (i<line.length) {
      val c = line.charAt(i)
      if (c=='"') {
        if (inQ && i+1<line.length && line.charAt(i+1)=='"') { sb.append('"'); i+=1 }
        else inQ = !inQ
      } else if (c==',' && !inQ) {
        res += sb.toString; sb.clear()
      } else sb.append(c)
      i+=1
    }
    res += sb.toString
    res.toList
  }
}

