package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem023QueueUsingDeque

class Problem023QueueUsingDequeSpec extends AnyFunSuite {
  test("queue") { val q=new Problem023QueueUsingDeque.QueueX[Int]; q.enqueue(1); q.enqueue(2); assert(q.dequeue()==1) }
}

