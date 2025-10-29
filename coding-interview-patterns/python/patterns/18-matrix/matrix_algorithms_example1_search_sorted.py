#!/usr/bin/env python3
"""Example 1: Search in a Row/Column-Sorted Matrix
Each row is sorted ascending left->right, and each column is sorted ascending top->bottom.
Use the top-right staircase scan in O(m+n) time and O(1) space.
"""
from typing import List

def search_sorted_matrix(M: List[List[int]], target: int) -> bool:
    if not M or not M[0]: 
        return False
    m, n = len(M), len(M[0])
    i, j = 0, n - 1
    # Invariant: everything left of j in row i is <= M[i][j];
    # everything below i in column j is >= M[i][j].
    while i < m and j >= 0:
        if M[i][j] == target:
            return True
        if M[i][j] > target:
            j -= 1   # move left to smaller values
        else:
            i += 1   # move down to larger values
    return False

def run_example_1():
    M = [
        [1, 4, 7, 11, 15],
        [2, 5, 8, 12, 19],
        [3, 6, 9, 16, 22],
        [10,13,14,17, 24],
        [18,21,23,26, 30],
    ]
    print("Example 1 — Search 5:", search_sorted_matrix(M, 5))   # True
    print("Example 1 — Search 20:", search_sorted_matrix(M, 20)) # False

if __name__ == "__main__":
    run_example_1()
