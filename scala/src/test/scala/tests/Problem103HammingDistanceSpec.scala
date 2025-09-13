
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem103HammingDistance

class Problem103HammingDistanceSpec extends AnyFunSuite {
  test("hamming") {
    assert(Problem103HammingDistance.hamming(1,4) === 2)
  }
}

