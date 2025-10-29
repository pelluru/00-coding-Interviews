#!/usr/bin/env python3
"""Example 3: Flood Fill (DFS on Grid) — recolor connected component
Change all cells connected 4-directionally to (sr, sc) with the original color to newColor.
Time: O(m*n) worst; Space: O(m*n) recursion worst.
"""
from typing import List

def flood_fill(image: List[List[int]], sr: int, sc: int, newColor: int) -> List[List[int]]:
    m, n = len(image), len(image[0])
    old = image[sr][sc]
    if old == newColor:
        return image
    def dfs(i: int, j: int) -> None:
        if i < 0 or i >= m or j < 0 or j >= n or image[i][j] != old:
            return
        image[i][j] = newColor
        dfs(i + 1, j); dfs(i - 1, j); dfs(i, j + 1); dfs(i, j - 1)
    dfs(sr, sc)
    return image

def run_example_3():
    img = [
        [1,1,1,2],
        [1,1,0,2],
        [1,0,1,2]
    ]
    print("Example 3 — Flood fill:", flood_fill(img, 1, 1, 9))

if __name__ == "__main__":
    run_example_3()
