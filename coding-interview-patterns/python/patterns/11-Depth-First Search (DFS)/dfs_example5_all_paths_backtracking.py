#!/usr/bin/env python3
"""Example 5: All Paths (Backtracking) using DFS
List all paths from a source to a destination in a directed graph.
"""
from typing import Dict, List
def dfs_paths(src: str, dst: str, graph: Dict[str, List[str]], path: List[str], out: List[List[str]]) -> None:
    path.append(src)
    if src == dst:
        out.append(list(path))
    else:
        for nbr in graph.get(src, []):
            dfs_paths(nbr, dst, graph, path, out)
    path.pop()
def run_example_5() -> None:
    graph: Dict[str, List[str]] = {'A': ['B', 'C'],'B': ['D', 'E'],'C': ['F'],'D': [],'E': ['F'],'F': []}
    all_paths: List[List[str]] = []
    dfs_paths('A', 'F', graph, [], all_paths)
    print("Example 5 — All paths from A to F:")
    for p in all_paths:
        print(" -> ".join(p))
if __name__ == "__main__":
    run_example_5()
