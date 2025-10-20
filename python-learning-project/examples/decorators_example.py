"""Decorator examples"""
def my_decorator(func):
    def wrapper(*args):
        print("Before")
        result = func(*args)
        print("After")
        return result
    return wrapper

@my_decorator
def greet(name):
    print(f"Hello {name}!")

greet("Alice")
