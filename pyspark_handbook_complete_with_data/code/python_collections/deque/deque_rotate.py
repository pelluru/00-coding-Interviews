"""
Deque example 3: rotation
Run: python deque_rotate.py
"""
from collections import deque

def main():
    d = deque([1,2,3,4])
    d.rotate(1)
    print(list(d))
    d.rotate(-2)
    print(list(d))

if __name__ == "__main__":
    main()
