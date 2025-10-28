#!/usr/bin/env python3
"""Example 3: DFS for Counting Connected Components in a Graph
Given an undirected graph, count how many connected components exist.
"""
from typing import Dict, List, Set
def dfs_component(node: int, graph: Dict[int, List[int]], visited: Set[int]) -> None:
    visited.add(node)
    for nbr in graph.get(node, []):
        if nbr not in visited:
            dfs_component(nbr, graph, visited)
def run_example_3() -> None:
    graph: Dict[int, List[int]] = {1: [2],2: [1, 3],3: [2],4: [5],5: [4],6: []}
    visited: Set[int] = set()
    components = 0
    for node in graph:
        if node not in visited:
            dfs_component(node, graph, visited)
            components += 1
    print(f"Example 3 — Number of connected components: {components}")
if __name__ == "__main__":
    run_example_3()
