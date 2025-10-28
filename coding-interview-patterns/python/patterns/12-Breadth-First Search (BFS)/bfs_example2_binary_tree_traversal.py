#!/usr/bin/env python3
"""Example 2: BFS Traversal on a Binary Tree (Level Order)
Goal:
- Traverse the tree level by level (breadth-first search).
"""
from collections import deque
from typing import Optional

class Node:
    def __init__(self, val: str):
        self.val = val
        self.left: Optional['Node'] = None
        self.right: Optional['Node'] = None

def bfs_tree(root: Optional[Node]) -> None:
    if not root: return
    q = deque([root])
    while q:
        node = q.popleft()
        print(node.val, end=" ")
        if node.left: q.append(node.left)
        if node.right: q.append(node.right)

def run_example_2() -> None:
    root = Node('A')
    root.left = Node('B')
    root.right = Node('C')
    root.left.left = Node('D')
    root.left.right = Node('E')
    root.right.right = Node('F')
    print("Example 2 — Binary Tree Level Order BFS:")
    bfs_tree(root)
    print()

if __name__ == "__main__":
    run_example_2()
