package problems
object Problem085RetryDecorator {
  def withRetry[T](times:Int)(op: => T): T = {
    var t=0; var last:Throwable=null
    while (t<times) { try return op; catch { case e:Throwable => last=e; t+=1 } }
    throw last
  }
}
