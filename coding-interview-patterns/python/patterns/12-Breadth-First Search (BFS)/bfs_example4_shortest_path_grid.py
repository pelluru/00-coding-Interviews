#!/usr/bin/env python3
"""Example 4: BFS on a 2D Grid (Shortest Distance in Matrix)
Goal:
- Find shortest distance from source to target in a binary matrix.
- 1 = walkable path, 0 = blocked cell.
"""
from collections import deque
from typing import List, Tuple

def bfs_shortest_distance(grid: List[List[int]], start: Tuple[int,int], end: Tuple[int,int]) -> int:
    rows, cols = len(grid), len(grid[0])
    q = deque([(start[0], start[1], 0)])
    visited = set([start])
    while q:
        r, c, dist = q.popleft()
        if (r, c) == end:
            return dist
        for dr, dc in [(1,0),(-1,0),(0,1),(0,-1)]:
            nr, nc = r+dr, c+dc
            if 0 <= nr < rows and 0 <= nc < cols and grid[nr][nc]==1 and (nr,nc) not in visited:
                visited.add((nr,nc))
                q.append((nr,nc,dist+1))
    return -1

def run_example_4() -> None:
    grid = [[1,1,0,1],[0,1,1,1],[0,0,1,0],[1,1,1,1]]
    distance = bfs_shortest_distance(grid,(0,0),(3,3))
    print(f"Example 4 — Shortest path distance in grid: {distance}")

if __name__ == "__main__":
    run_example_4()
