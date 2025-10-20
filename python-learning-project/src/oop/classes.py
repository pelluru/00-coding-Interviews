"""Class examples"""
class Dog:
    def __init__(self, name, age):
        self.name = name
        self.age = age
    
    def bark(self):
        return f"{self.name} says Woof!"
    
    def __str__(self):
        return f"Dog({self.name}, {self.age})"

if __name__ == "__main__":
    dog = Dog("Buddy", 3)
    print(dog)
    print(dog.bark())
