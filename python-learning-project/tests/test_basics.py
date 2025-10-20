"""Basic tests"""
import pytest

def test_list_operations():
    fruits = ["apple", "banana"]
    fruits.append("cherry")
    assert len(fruits) == 3
    assert "banana" in fruits

def test_dict_operations():
    person = {"name": "John", "age": 30}
    assert person["name"] == "John"
