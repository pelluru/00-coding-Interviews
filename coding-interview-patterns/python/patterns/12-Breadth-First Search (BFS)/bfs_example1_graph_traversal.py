#!/usr/bin/env python3
"""Example 1: BFS Traversal on a Graph (Adjacency List)
Goal:
- Perform a BFS traversal starting from a given node and print visit order.
- Use a queue to explore level by level (breadth-first).
"""
from collections import deque
from typing import Dict, List, Set

def bfs_graph(start: str, graph: Dict[str, List[str]]) -> None:
    visited: Set[str] = set([start])
    q = deque([start])
    while q:
        node = q.popleft()
        print(node, end=" ")
        for neighbor in graph.get(node, []):
            if neighbor not in visited:
                visited.add(neighbor)
                q.append(neighbor)

def run_example_1() -> None:
    graph = {'A': ['B', 'C'],'B': ['A', 'D', 'E'],'C': ['A', 'F'],'D': ['B'],'E': ['B', 'F'],'F': ['C', 'E']}
    print("Example 1 — BFS Traversal starting from 'A':")
    bfs_graph('A', graph)
    print()

if __name__ == "__main__":
    run_example_1()
