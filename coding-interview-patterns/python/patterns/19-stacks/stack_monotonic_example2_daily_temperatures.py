#!/usr/bin/env python3
"""Example 2: Daily Temperatures
For each day, find how many days to wait until a warmer temperature; else 0.

Algorithm:
- Stack holds indices of days with decreasing temperatures.
- On seeing a warmer day, pop indices and set distance (i - j).
Time: O(n), Space: O(n).
"""
from typing import List

def daily_temperatures(T: List[int]) -> List[int]:
    st: List[int] = []
    res: List[int] = [0] * len(T)
    for i, t in enumerate(T):
        while st and T[st[-1]] < t:
            j = st.pop()
            res[j] = i - j
        st.append(i)
    return res

if __name__ == "__main__":
    print("Example 2 — Daily Temperatures:", daily_temperatures([73,74,75,71,69,72,76,73]))  # [1,1,4,2,1,1,0,0]
