#!/usr/bin/env python3
"""Example 5: Letter Combinations of a Phone Number (Backtracking)
Given digits '2'..'9', return all possible letter combinations using phone mapping.
Backtracking idea:
- Build the string one digit at a time; try each letter mapped to the current digit.
- When the length equals digits length, record the combination and backtrack.
"""
from typing import List

def letter_combinations(digits: str) -> List[str]:
    if not digits:
        return []
    phone = {
        '2': 'abc', '3': 'def',
        '4': 'ghi', '5': 'jkl', '6': 'mno',
        '7': 'pqrs', '8': 'tuv', '9': 'wxyz'
    }
    res: List[str] = []
    path: List[str] = []

    def dfs(i: int) -> None:
        if i == len(digits):
            res.append(''.join(path))
            return
        d = digits[i]
        for ch in phone[d]:
            path.append(ch)   # choose
            dfs(i + 1)        # explore
            path.pop()        # backtrack

    dfs(0)
    return res

def run_example_5() -> None:
    print("Example 5 — Letter combinations for '23':", letter_combinations("23"))

if __name__ == "__main__":
    run_example_5()
