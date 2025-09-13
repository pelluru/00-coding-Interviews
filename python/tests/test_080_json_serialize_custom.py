import unittest
from problems.json_serialize_custom import json_serialize_custom
class TestJsonSerializeCustom (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(json_serialize_custom())
