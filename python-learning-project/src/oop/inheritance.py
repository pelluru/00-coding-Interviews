"""Inheritance examples"""
class Animal:
    def __init__(self, name, species):
        self.name = name
        self.species = species
    
    def make_sound(self):
        return "Some sound"
    
    def info(self):
        return f"{self.name} is a {self.species}"

class Cat(Animal):
    def __init__(self, name, color):
        super().__init__(name, "Cat")
        self.color = color
    
    def make_sound(self):
        return "Meow!"
    
    def purr(self):
        return f"{self.name} is purring"

if __name__ == "__main__":
    cat = Cat("Whiskers", "orange")
    print(cat.info())
    print(cat.make_sound())
    print(cat.purr())
