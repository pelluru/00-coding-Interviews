#!/usr/bin/env python3
"""Example 2: Rotate Image 90° Clockwise In-Place
Algorithm:
  1) Transpose the matrix in-place (swap mat[i][j] with mat[j][i] for j>i).
  2) Reverse each row in-place.
Time: O(n^2), Space: O(1) extra.
"""
from typing import List

def rotate_image(mat: List[List[int]]) -> List[List[int]]:
    n = len(mat)
    # transpose
    for i in range(n):
        for j in range(i + 1, n):
            mat[i][j], mat[j][i] = mat[j][i], mat[i][j]
    # reverse each row
    for row in mat:
        row.reverse()
    return mat

def run_example_2():
    M = [
        [1, 2, 3],
        [4, 5, 6],
        [7, 8, 9],
    ]
    print("Example 2 — Rotate 3x3: before ->", M)
    print("Example 2 — Rotate 3x3: after  ->", rotate_image(M))  # [[7,4,1],[8,5,2],[9,6,3]]

if __name__ == "__main__":
    run_example_2()
