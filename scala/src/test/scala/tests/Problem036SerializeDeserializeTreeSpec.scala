
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem036SerializeDeserializeTree

class Problem036SerializeDeserializeTreeSpec extends AnyFunSuite {
  test("serdes") {
    val r = Problem036SerializeDeserializeTree.Node(1); r.l=Problem036SerializeDeserializeTree.Node(2); r.r=Problem036SerializeDeserializeTree.Node(3)
    val s = Problem036SerializeDeserializeTree.serialize(r)
    assert(Problem036SerializeDeserializeTree.deserialize(s) ne null)
  }
}

