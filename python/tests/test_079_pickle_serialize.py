import unittest
from problems.pickle_serialize import pickle_serialize
class TestPickleSerialize (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(pickle_serialize())
