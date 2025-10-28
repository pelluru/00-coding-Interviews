#!/usr/bin/env python3
"""Example 1: DFS Traversal on a Graph (Adjacency List)
Goal:
- Perform a DFS traversal starting from a given node and print the visit order.
Key ideas:
- Use recursion to go deep (depth-first) along neighbors.
- Maintain a 'visited' set to avoid revisiting nodes / infinite loops.
- This works for directed or undirected graphs; we assume undirected here.
"""
from typing import Dict, List, Set
def dfs_graph(node: str, graph: Dict[str, List[str]], visited: Set[str]) -> None:
    """Recursive DFS on a graph."""
    visited.add(node)
    print(node, end=" ")
    for neighbor in graph.get(node, []):
        if neighbor not in visited:
            dfs_graph(neighbor, graph, visited)
def run_example_1() -> None:
    graph = {'A': ['B', 'C'],'B': ['A', 'D', 'E'],'C': ['A', 'F'],'D': ['B'],'E': ['B', 'F'],'F': ['C', 'E']}
    visited: Set[str] = set()
    print("Example 1 — DFS Traversal starting from 'A':")
    dfs_graph('A', graph, visited)
    print()
if __name__ == "__main__":
    run_example_1()
