import unittest
from problems.token_bucket_limiter import token_bucket_limiter
class TestTokenBucketLimiter (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(token_bucket_limiter())
