#!/usr/bin/env python3
"""Example 4: Counting Islands on a 2D Grid using DFS
'1' = land, '0' = water. Count distinct islands (4-directional connectivity).
"""
from typing import List
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
if __name__ == "__main__":
    run_example_4()
