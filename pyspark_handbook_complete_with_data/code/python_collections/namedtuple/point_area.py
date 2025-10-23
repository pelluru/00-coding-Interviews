"""
namedtuple example 1: simple record
Run: python point_area.py
"""
from collections import namedtuple

def main():
    Point = namedtuple("Point", "x y")
    p = Point(3, 4)
    print(p.x * p.y)

if __name__ == "__main__":
    main()
