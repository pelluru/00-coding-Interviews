import unittest
from problems.trie_insert_search import trie_insert_search
class TestTrieInsertSearch (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(trie_insert_search())
