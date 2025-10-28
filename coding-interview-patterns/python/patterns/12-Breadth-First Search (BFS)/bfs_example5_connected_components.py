#!/usr/bin/env python3
"""Example 5: BFS for Connected Components (Iterative)
Goal:
- Count connected components in an undirected graph using BFS.
"""
from collections import deque
from typing import Dict, List, Set

def bfs_component(start: int, graph: Dict[int, List[int]], visited: Set[int]) -> None:
    q = deque([start])
    visited.add(start)
    while q:
        node = q.popleft()
        for nbr in graph.get(node, []):
            if nbr not in visited:
                visited.add(nbr)
                q.append(nbr)

def run_example_5() -> None:
    graph = {1:[2],2:[1,3],3:[2],4:[5],5:[4],6:[]}
    visited=set(); components=0
    for node in graph:
        if node not in visited:
            bfs_component(node,graph,visited)
            components+=1
    print(f"Example 5 — Number of connected components: {components}")

if __name__ == "__main__":
    run_example_5()
