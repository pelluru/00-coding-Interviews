import unittest
from problems.deserialize_graph_adjlist import deserialize_graph_adjlist
class TestDeserializeGraphAdjlist (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(deserialize_graph_adjlist())
