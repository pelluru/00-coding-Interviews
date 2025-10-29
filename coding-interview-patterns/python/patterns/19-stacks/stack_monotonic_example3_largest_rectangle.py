#!/usr/bin/env python3
"""Example 3: Largest Rectangle in Histogram
Use a monotonic increasing stack of indices. Append a sentinel 0 to flush the stack.

Algorithm:
- While current bar height x is less than height at stack top, pop H and compute width using the new top.
- Width = i - L - 1, where L = stack[-1] after pop, or -1 if empty.
Time: O(n), Space: O(n).
"""
from typing import List

def largest_rectangle(h: List[int]) -> int:
    st: List[int] = []
    ans = 0
    h.append(0)  # sentinel to flush
    for i, x in enumerate(h):
        while st and h[st[-1]] > x:
            H = h[st.pop()]
            L = st[-1] if st else -1
            ans = max(ans, H * (i - L - 1))
        st.append(i)
    h.pop()
    return ans

if __name__ == "__main__":
    print("Example 3 — Largest Rectangle:", largest_rectangle([2,1,5,6,2,3]))  # 10
