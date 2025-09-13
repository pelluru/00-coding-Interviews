
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem099UrlValidation

class Problem099UrlValidationSpec extends AnyFunSuite {
  test("url validation") {
    assert(Problem099UrlValidation.isValid("https://example.com"))
    assert(!Problem099UrlValidation.isValid("ht!tp://bad"))
  }
}

