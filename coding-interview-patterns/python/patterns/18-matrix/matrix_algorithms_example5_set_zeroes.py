#!/usr/bin/env python3
"""Example 5: Set Matrix Zeroes (In-Place, O(1) Extra Space)
If an element is 0, set its entire row and column to 0.
Use first row and first column as markers; track two booleans for their original zeros.
Time: O(m*n), Space: O(1) extra.
"""
from typing import List

def set_zeroes(mat: List[List[int]]) -> None:
    if not mat or not mat[0]:
        return
    m, n = len(mat), len(mat[0])
    first_row_zero = any(mat[0][c] == 0 for c in range(n))
    first_col_zero = any(mat[r][0] == 0 for r in range(m))

    # mark zeros using first row/col
    for r in range(1, m):
        for c in range(1, n):
            if mat[r][c] == 0:
                mat[r][0] = 0
                mat[0][c] = 0

    # zero inner cells based on markers
    for r in range(1, m):
        for c in range(1, n):
            if mat[r][0] == 0 or mat[0][c] == 0:
                mat[r][c] = 0

    # zero first row/col if needed
    if first_row_zero:
        for c in range(n):
            mat[0][c] = 0
    if first_col_zero:
        for r in range(m):
            mat[r][0] = 0

def run_example_5():
    M = [
        [1, 1, 1],
        [1, 0, 1],
        [1, 1, 1],
    ]
    set_zeroes(M)
    print("Example 5 — Set matrix zeroes:", M)  # [[1,0,1],[0,0,0],[1,0,1]]

if __name__ == "__main__":
    run_example_5()
