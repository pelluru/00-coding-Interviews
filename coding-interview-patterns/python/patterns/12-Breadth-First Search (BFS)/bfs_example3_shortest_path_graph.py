#!/usr/bin/env python3
"""Example 3: Shortest Path in an Unweighted Graph using BFS
Goal:
- Find shortest path (minimum edges) from source to destination.
"""
from collections import deque
from typing import Dict, List, Optional

def bfs_shortest_path(graph: Dict[str, List[str]], start: str, end: str) -> Optional[List[str]]:
    q = deque([(start, [start])])
    visited = set([start])
    while q:
        node, path = q.popleft()
        if node == end:
            return path
        for nbr in graph.get(node, []):
            if nbr not in visited:
                visited.add(nbr)
                q.append((nbr, path + [nbr]))
    return None

def run_example_3() -> None:
    graph = {'A':['B','C'],'B':['D','E'],'C':['F'],'D':[],'E':['F'],'F':[]}
    path = bfs_shortest_path(graph, 'A', 'F')
    print("Example 3 — Shortest path from A to F:", " -> ".join(path) if path else "No path")

if __name__ == "__main__":
    run_example_3()
