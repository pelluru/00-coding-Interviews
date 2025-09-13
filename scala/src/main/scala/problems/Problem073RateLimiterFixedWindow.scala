package problems


object Problem073RateLimiterFixedWindow {
  class Limiter(limit:Int, windowMillis:Long) {
    private var windowStart = 0L
    private var count = 0
    def allow(now:Long): Boolean = {
      if (now - windowStart >= windowMillis) { windowStart = now; count = 0 }
      if (count < limit) { count += 1; true } else false
    }
  }
}

