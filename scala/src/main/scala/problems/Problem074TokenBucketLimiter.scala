package problems


object Problem074TokenBucketLimiter {
  class TokenBucket(ratePerSec:Double, capacity:Int) {
    private var tokens: Double = capacity
    private var last: Long = 0L
    def allow(nowMillis:Long): Boolean = {
      if (last==0L) last = nowMillis
      val delta = (nowMillis - last) / 1000.0
      tokens = math.min(capacity, tokens + delta * ratePerSec)
      last = nowMillis
      if (tokens >= 1.0) { tokens -= 1.0; true } else false
    }
  }
}

