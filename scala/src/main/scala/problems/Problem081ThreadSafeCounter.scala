package problems
import java.util.concurrent.atomic.AtomicLong
object Problem081ThreadSafeCounter {
  class Counter { private val c = new AtomicLong(0); def inc():Long=c.incrementAndGet(); def get:Long=c.get() }
}
