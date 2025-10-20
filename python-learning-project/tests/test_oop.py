"""OOP tests"""
import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from oop.classes import Dog

def test_dog_creation():
    dog = Dog("Buddy", 3)
    assert dog.name == "Buddy"
    assert dog.age == 3
