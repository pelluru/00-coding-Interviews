#!/usr/bin/env python3
"""Example 2: DFS Traversal on a Binary Tree (Preorder)
Traversals (all are DFS variants):
- Preorder:  Root -> Left -> Right
- Inorder:   Left -> Root -> Right
- Postorder: Left -> Right -> Root
We implement Preorder here and print the visit order.
"""
from typing import Optional
class Node:
    def __init__(self, val: str):
        self.val = val
        self.left: Optional['Node'] = None
        self.right: Optional['Node'] = None
def preorder(node: Optional[Node]) -> None:
    if not node:
        return
    print(node.val, end=" ")
    preorder(node.left)
    preorder(node.right)
def run_example_2() -> None:
    #        A
    #       / \\
    #      B   C
    #     / \\   \\
    #    D   E    F
    root = Node('A')
    root.left = Node('B')
    root.right = Node('C')
    root.left.left = Node('D')
    root.left.right = Node('E')
    root.right.right = Node('F')
    print("Example 2 — Binary Tree Preorder DFS:")
    preorder(root)
    print()
if __name__ == "__main__":
    run_example_2()
