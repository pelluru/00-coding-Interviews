package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem083ProducerConsumerQueue

class Problem083ProducerConsumerQueueSpec extends AnyFunSuite {
  test("pc queue") { val pc=new Problem083ProducerConsumerQueue.PC(); pc.produce(1); assert(pc.consume()==1) }
}

