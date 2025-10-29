#!/usr/bin/env python3
"""Example 4: Spiral Order Traversal (Clockwise)
Walk the matrix in layers using four bounds (top, bottom, left, right).
Time: O(m*n), Space: O(1) extra (ignoring output).
"""
from typing import List

def spiral_order(mat: List[List[int]]) -> List[int]:
    if not mat or not mat[0]:
        return []
    top, bottom = 0, len(mat) - 1
    left, right = 0, len(mat[0]) - 1
    out = []
    while top <= bottom and left <= right:
        for c in range(left, right + 1):
            out.append(mat[top][c])
        top += 1
        for r in range(top, bottom + 1):
            out.append(mat[r][right])
        right -= 1
        if top <= bottom:
            for c in range(right, left - 1, -1):
                out.append(mat[bottom][c])
            bottom -= 1
        if left <= right:
            for r in range(bottom, top - 1, -1):
                out.append(mat[r][left])
            left += 1
    return out

def run_example_4():
    M = [
        [1,  2,  3,  4],
        [5,  6,  7,  8],
        [9, 10, 11, 12],
        [13,14, 15, 16],
    ]
    print("Example 4 — Spiral order:", spiral_order(M))

if __name__ == "__main__":
    run_example_4()
