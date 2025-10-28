#!/usr/bin/env python3
"""Example 4: N-Queens (Backtracking)
Place n queens on an n×n board so that no two attack each other.
Backtracking idea:
- Place one queen per row, track threatened columns and diagonals.
- Recurse row-by-row; if a row has no valid columns, backtrack.
"""
from typing import List, Set

def solve_n_queens(n: int) -> List[List[str]]:
    res: List[List[str]] = []
    # columns and diagonals in use
    cols: Set[int] = set()
    diag1: Set[int] = set()  # r - c
    diag2: Set[int] = set()  # r + c
    board = [['.' for _ in range(n)] for _ in range(n)]

    def dfs(r: int) -> None:
        # Base: all rows filled
        if r == n:
            res.append([''.join(row) for row in board])
            return
        for c in range(n):
            if c in cols or (r - c) in diag1 or (r + c) in diag2:
                continue
            # choose
            cols.add(c); diag1.add(r - c); diag2.add(r + c)
            board[r][c] = 'Q'
            # explore next row
            dfs(r + 1)
            # backtrack
            board[r][c] = '.'
            cols.remove(c); diag1.remove(r - c); diag2.remove(r + c)

    dfs(0)
    return res

def run_example_4() -> None:
    n = 4
    solutions = solve_n_queens(n)
    print(f"Example 4 — N-Queens solutions for n={n} (count={len(solutions)}):")
    for sol in solutions:
        for row in sol:
            print(row)
        print('-' * n)

if __name__ == "__main__":
    run_example_4()
