#!/usr/bin/env python3
"""Example 4: Stock Span
For each day's price, compute the span of consecutive days (including today) with price <= today's.

Algorithm:
- Maintain a stack of (index, price) strictly decreasing by price.
- Pop while stack top price <= current price to extend span.
- Span = i - last_greater_index.
Time: O(n), Space: O(n).
"""
from typing import List, Tuple

def stock_span(prices: List[int]) -> List[int]:
    st: List[Tuple[int, int]] = []  # (index, price)
    res = [0] * len(prices)
    for i, p in enumerate(prices):
        while st and st[-1][1] <= p:
            st.pop()
        last_greater_idx = st[-1][0] if st else -1
        res[i] = i - last_greater_idx
        st.append((i, p))
    return res

if __name__ == "__main__":
    print("Example 4 — Stock Span:", stock_span([100,80,60,70,60,75,85]))  # [1,1,1,2,1,4,6]
