package problems
object Problem084ExponentialBackoffRetry {
  def retry[T](times:Int, baseMillis:Long=5)(op: => T): T = {
    var attempt=0
    var delay=baseMillis
    var last: Throwable = null
    while (attempt < times) {
      try return op
      catch { case t: Throwable => last=t; Thread.sleep(delay); delay=delay*2; attempt+=1 }
    }
    throw last
  }
}
