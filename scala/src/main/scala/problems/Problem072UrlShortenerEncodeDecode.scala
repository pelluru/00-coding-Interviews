package problems


object Problem072UrlShortenerEncodeDecode {
  private var id: Long = 0
  private val store = scala.collection.mutable.HashMap[Long,String]()
  private val chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
  def encode(url:String): String = {
    id += 1; store(id)=url; base62(id)
  }
  def decode(code:String): String = store(base62inv(code))
  private def base62(n:Long): String = {
    var x=n; val sb=new StringBuilder; if (x==0) return "0"
    while (x>0) { sb.append(chars.charAt((x % 62).toInt)); x/=62 }
    sb.reverse.toString
  }
  private def base62inv(s:String): Long = s.foldLeft(0L)((acc,c)=> acc*62 + chars.indexOf(c))
}

