
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem100EmailValidationRegex

class Problem100EmailValidationRegexSpec extends AnyFunSuite {
  test("email validation") {
    assert(Problem100EmailValidationRegex.isValid("a@b.com"))
    assert(!Problem100EmailValidationRegex.isValid("not-an-email"))
  }
}

