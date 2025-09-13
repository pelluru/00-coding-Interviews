package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem046SieveEratosthenes

class Problem046SieveEratosthenesSpec extends AnyFunSuite {
  test("sieve 10") {
    assert(Problem046SieveEratosthenes.sieve(10) === List(2,3,5,7))
  }
}
