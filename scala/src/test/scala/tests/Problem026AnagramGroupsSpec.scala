package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem026AnagramGroups

class Problem026AnagramGroupsSpec extends AnyFunSuite {
  test("anagram groups size") { val g=Problem026AnagramGroups.group(Array("eat","tea","tan","ate","nat","bat")); assert(g.map(_.size).sum==6) }
}

