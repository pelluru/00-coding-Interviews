#!/usr/bin/env python3
"""DFS — Five Examples in One File

Contains five independent examples demonstrating Depth-First Search (DFS):
  1) Graph DFS traversal (adjacency list)
  2) Binary tree preorder traversal
  3) Counting connected components in a graph
  4) Counting islands in a 2D grid
  5) Enumerating all paths (backtracking)

Usage:
  python dfs_all_examples.py           # run all
  python dfs_all_examples.py 1 3 5     # run selected examples
"""
from typing import List, Dict, Set, Optional

# Example 1
def dfs_graph(node: str, graph: Dict[str, List[str]], visited: Set[str]) -> None:
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

# Example 2
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
    root = Node('A')
    root.left = Node('B')
    root.right = Node('C')
    root.left.left = Node('D')
    root.left.right = Node('E')
    root.right.right = Node('F')
    print("Example 2 — Binary Tree Preorder DFS:")
    preorder(root)
    print()

# Example 3
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

# Example 4
def dfs_island(grid: List[List[str]], r: int, c: int) -> None:
    rows, cols = len(grid), len(grid[0])
    if r < 0 or c < 0 or r >= rows or c >= cols or grid[r][c] != '1':
        return
    grid[r][c] = '#'
    dfs_island(grid, r+1, c)
    dfs_island(grid, r-1, c)
    dfs_island(grid, r, c+1)
    dfs_island(grid, r, c-1)
def count_islands(grid: List[List[str]]) -> int:
    if not grid or not grid[0]:
        return 0
    rows, cols = len(grid), len(grid[0])
    count = 0
    for r in range(rows):
        for c in range(cols):
            if grid[r][c] == '1':
                dfs_island(grid, r, c)
                count += 1
    return count
def run_example_4() -> None:
    grid = [['1','1','0','0','0'],['1','0','0','1','1'],['0','0','1','1','0'],['0','0','0','0','0']]
    ans = count_islands(grid)
    print(f"Example 4 — Number of islands: {ans}")

# Example 5
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

def _run_all(selected=None):
    mapping = {1: run_example_1, 2: run_example_2, 3: run_example_3, 4: run_example_4, 5: run_example_5}
    order = selected or [1,2,3,4,5]
    for i in order:
        if i in mapping:
            mapping[i]()
        else:
            print(f"Unknown example index: {i}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) == 1:
        _run_all()
    else:
        try:
            selected = [int(x) for x in sys.argv[1:]]
        except ValueError:
            print("Usage: python dfs_all_examples.py [example_numbers...]  # e.g., 1 3 5")
            raise SystemExit(1)
        _run_all(selected)
