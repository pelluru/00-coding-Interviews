package problems
import java.util.concurrent.ArrayBlockingQueue
object Problem083ProducerConsumerQueue {
  class PC(cap:Int=16) {
    private val q = new ArrayBlockingQueue[Int](cap)
    def produce(x:Int): Unit = q.put(x)
    def consume(): Int = q.take()
    def size: Int = q.size()
  }
}
