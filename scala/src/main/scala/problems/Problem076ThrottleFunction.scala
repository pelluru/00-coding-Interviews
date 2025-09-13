package problems


object Problem076ThrottleFunction {
  class Throttle(limit:Int, windowMillis:Long) {
    private var times = List[Long]()
    def allow(now:Long): Boolean = {
      times = (now :: times).filter(t => now - t < windowMillis)
      if (times.length <= limit) true else { times = times.tail; false }
    }
  }
}

