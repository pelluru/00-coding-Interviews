
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem009LinkedListCycle

class Problem009LinkedListCycleSpec extends AnyFunSuite {
  test("cycle") {
    val a = Problem009LinkedListCycle.Node(1); val b = Problem009LinkedListCycle.Node(2); val c = Problem009LinkedListCycle.Node(3)
    a.next=b; b.next=c; c.next=b
    assert(Problem009LinkedListCycle.hasCycle(a))
  }
}

