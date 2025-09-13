package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem075DebounceFunction

class Problem075DebounceFunctionSpec extends AnyFunSuite {
  test("debounce") { val d=new Problem075DebounceFunction.Debounce(100); assert(d.run(0){()}, "first ok"); assert(!d.run(50){()}); assert(d.run(200){()}) }
}

