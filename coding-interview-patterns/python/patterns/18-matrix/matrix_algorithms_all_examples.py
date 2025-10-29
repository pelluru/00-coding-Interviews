#!/usr/bin/env python3
"""Matrix Algorithms — Five Examples in One File
1) search_sorted_matrix : O(m+n) staircase search
2) rotate_image         : transpose + reverse rows
3) flood_fill           : DFS recolor
4) spiral_order         : layer traversal
5) set_zeroes           : O(1) extra space markers
"""
from typing import List

def search_sorted_matrix(M: List[List[int]], target: int) -> bool:
    if not M or not M[0]: return False
    m, n = len(M), len(M[0]); i, j = 0, n - 1
    while i < m and j >= 0:
        if M[i][j] == target: return True
        if M[i][j] > target: j -= 1
        else: i += 1
    return False

def rotate_image(mat: List[List[int]]) -> List[List[int]]:
    n = len(mat)
    for i in range(n):
        for j in range(i + 1, n):
            mat[i][j], mat[j][i] = mat[j][i], mat[i][j]
    for row in mat: row.reverse()
    return mat

def flood_fill(image: List[List[int]], sr: int, sc: int, newColor: int) -> List[List[int]]:
    m, n = len(image), len(image[0]); old = image[sr][sc]
    if old == newColor: return image
    def dfs(i: int, j: int):
        if i < 0 or i >= m or j < 0 or j >= n or image[i][j] != old: return
        image[i][j] = newColor
        dfs(i+1,j); dfs(i-1,j); dfs(i,j+1); dfs(i,j-1)
    dfs(sr, sc); return image

def spiral_order(mat: List[List[int]]) -> List[int]:
    if not mat or not mat[0]: return []
    top, bottom, left, right = 0, len(mat)-1, 0, len(mat[0])-1; out=[]
    while top <= bottom and left <= right:
        for c in range(left, right+1): out.append(mat[top][c])
        top += 1
        for r in range(top, bottom+1): out.append(mat[r][right])
        right -= 1
        if top <= bottom:
            for c in range(right, left-1, -1): out.append(mat[bottom][c])
            bottom -= 1
        if left <= right:
            for r in range(bottom, top-1, -1): out.append(mat[r][left])
            left += 1
    return out

def set_zeroes(mat: List[List[int]]) -> None:
    if not mat or not mat[0]: return
    m, n = len(mat), len(mat[0])
    first_row_zero = any(mat[0][c] == 0 for c in range(n))
    first_col_zero = any(mat[r][0] == 0 for r in range(m))
    for r in range(1, m):
        for c in range(1, n):
            if mat[r][c] == 0:
                mat[r][0] = 0; mat[0][c] = 0
    for r in range(1, m):
        for c in range(1, n):
            if mat[r][0] == 0 or mat[0][c] == 0:
                mat[r][c] = 0
    if first_row_zero:
        for c in range(n): mat[0][c] = 0
    if first_col_zero:
        for r in range(m): mat[r][0] = 0

def _run_all(selected=None):
    def ex1():
        M=[[1,4,7,11,15],[2,5,8,12,19],[3,6,9,16,22],[10,13,14,17,24],[18,21,23,26,30]]
        print("Example 1 — Search 5:", search_sorted_matrix(M,5))
        print("Example 1 — Search 20:", search_sorted_matrix(M,20))
    def ex2():
        M=[[1,2,3],[4,5,6],[7,8,9]]; print("Example 2 — Rotate:", rotate_image(M))
    def ex3():
        img=[[1,1,1,2],[1,1,0,2],[1,0,1,2]]; print("Example 3 — Flood fill:", flood_fill(img,1,1,9))
    def ex4():
        M=[[1,2,3,4],[5,6,7,8],[9,10,11,12],[13,14,15,16]]; print("Example 4 — Spiral:", spiral_order(M))
    def ex5():
        M=[[1,1,1],[1,0,1],[1,1,1]]; set_zeroes(M); print("Example 5 — Set zeroes:", M)
    mapping={1:ex1,2:ex2,3:ex3,4:ex4,5:ex5}
    order = selected or [1,2,3,4,5]
    for i in order: mapping[i]()

if __name__ == "__main__":
    import sys
    if len(sys.argv)==1: _run_all()
    else: _run_all([int(x) for x in sys.argv[1:]])
