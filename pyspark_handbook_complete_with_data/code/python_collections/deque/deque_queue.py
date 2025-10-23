"""
Deque example 1: efficient queue
Run: python deque_queue.py
"""
from collections import deque

def main():
    q = deque([1,2,3])
    q.appendleft(0)
    q.append(4)
    print("popleft:", q.popleft(), "remaining:", list(q))

if __name__ == "__main__":
    main()
