import unittest
from problems.serialize_deserialize_tree import serialize_deserialize_tree
class TestSerializeDeserializeTree (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(serialize_deserialize_tree())
