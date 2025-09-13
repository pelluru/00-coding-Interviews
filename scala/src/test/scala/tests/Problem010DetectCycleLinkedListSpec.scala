package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem010DetectCycleLinkedList

class Problem010DetectCycleLinkedListSpec extends AnyFunSuite {
  test("cycle") {
    val a=Problem010DetectCycleLinkedList.Node(1); val b=Problem010DetectCycleLinkedList.Node(2); val c=Problem010DetectCycleLinkedList.Node(3); a.next=b; b.next=c; c.next=b
    assert(Problem010DetectCycleLinkedList.hasCycle(a))
  }
}

