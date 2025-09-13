import unittest
from problems.is_palindrome import is_palindrome
class TestIsPalindrome(unittest.TestCase):
    def test_examples(self):
        self.assertTrue(is_palindrome('A man, a plan, a canal: Panama'))
        self.assertFalse(is_palindrome('hello'))
