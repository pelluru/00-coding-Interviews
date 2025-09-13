import unittest
from problems.url_shortener_encode_decode import url_shortener_encode_decode
class TestUrlShortenerEncodeDecode (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(url_shortener_encode_decode())
