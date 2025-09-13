import unittest
from problems.producer_consumer_queue import producer_consumer_queue
class TestProducerConsumerQueue (unittest.TestCase):
    def test_placeholder(self):
        self.assertIsNone(producer_consumer_queue())
