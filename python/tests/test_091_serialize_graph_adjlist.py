import unittest
from problems.serialize_graph_adjlist import serialize_graph_adjlist
class TestSerializeGraphAdjlist (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(serialize_graph_adjlist())
