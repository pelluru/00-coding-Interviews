"""
Best Time to Buy and Sell Stock (Single Transaction)
----------------------------------------------------
Problem:
Given an array `prices` where `prices[i]` is the stock price on day i,
find the maximum profit from one buy and one sell.
If no profit is possible, return 0.

Algorithm (Greedy Single-Pass):
--------------------------------
1️⃣ Initialize:
    - `mn` = +∞  (minimum price so far)
    - `ans` = 0  (maximum profit found so far)

2️⃣ Iterate through each price `p` in prices:
    - Update minimum price if we find a lower one: `mn = min(mn, p)`
    - Compute today's profit if we sell today: `profit = p - mn`
    - Update the best profit: `ans = max(ans, profit)`

3️⃣ Return `ans` (the best profit found).

Why it works:
-------------
We continuously track the best buy price (`mn`) so far,
and compute potential profit by selling at the current price.

Complexity:
-----------
✅ Time: O(n) — One pass through the list  
✅ Space: O(1) — Constant extra variables

Edge Cases:
-----------
- Empty prices → return 0
- Strictly decreasing prices → profit = 0
- Only one price → profit = 0
"""

from typing import List

def max_profit(prices: List[int]) -> int:
    """Return max profit from one buy-sell transaction."""
    mn = float('inf')  # minimum price seen so far
    ans = 0            # best profit found
    for p in prices:
        # update min price
        if p < mn:
            mn = p
        # check profit if selling today
        profit = p - mn
        if profit > ans:
            ans = profit
    return ans


def main():
    """Demonstration of max_profit with example test cases."""
    print("Example 1: [7,1,5,3,6,4] →", max_profit([7,1,5,3,6,4]))  # 5
    print("Example 2: [7,6,4,3,1] →", max_profit([7,6,4,3,1]))      # 0
    print("Edge case: [] →", max_profit([]))                        # 0
    print("Edge case: [2] →", max_profit([2]))                      # 0


if __name__ == "__main__":
    main()

