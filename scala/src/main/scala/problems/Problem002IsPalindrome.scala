package problems

object Problem002IsPalindrome {
  def isPalindrome(s: String): Boolean = {
    if (s == null) false else s == s.reverse
  }
}
