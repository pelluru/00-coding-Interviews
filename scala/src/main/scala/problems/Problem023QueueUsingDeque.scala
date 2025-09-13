package problems
object Problem023QueueUsingDeque {
  class QueueX[T] { private val q = scala.collection.mutable.ArrayDeque[T](); def enqueue(x:T)=q.append(x); def dequeue():T=q.removeHead(); def isEmpty:Boolean=q.isEmpty }
}
