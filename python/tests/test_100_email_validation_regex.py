import unittest
from problems.email_validation_regex import email_validation_regex
class TestEmailValidationRegex (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(email_validation_regex())
