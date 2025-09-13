package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem031EditDistance

class Problem031EditDistanceSpec extends AnyFunSuite {
  test("edit distance") {
    assert(Problem031EditDistance.edit("kitten","sitting") === 3)
  }
}
