"""
Best Time to Buy and Sell Stock II (Unlimited Transactions)
-----------------------------------------------------------
Problem:
You are given an array `prices` where `prices[i]` is the price of a stock on day i.
You may complete as many transactions as you like (buy one and sell one share multiple times),
but you cannot hold multiple shares simultaneously (must sell before buying again).

Goal:
Return the maximum profit you can achieve.

Algorithm (Greedy Summation of Positive Gains):
------------------------------------------------
1️⃣ Idea:
   - Every time there is an increase from day i−1 to day i (prices[i] > prices[i-1]),
     we can take advantage of it by buying on day i−1 and selling on day i.
   - Summing all such positive differences gives the optimal profit.

2️⃣ Steps:
   - Initialize total profit = 0
   - For each day i from 1 to n−1:
       - If prices[i] > prices[i−1], add (prices[i] − prices[i−1]) to profit.
   - Return total profit.

Why it works:
-------------
Greedy works because local increases can always be combined
to form the global optimal set of buy/sell pairs.

Example:
---------
prices = [7,1,5,3,6,4]
profit = (5−1) + (6−3) = 7

Complexity:
------------
✅ Time: O(n) — single traversal  
✅ Space: O(1) — only constant variables

Edge Cases:
------------
- Empty array → 0
- Decreasing prices → 0
- Single element → 0
"""

from typing import List

def max_profit2(prices: List[int]) -> int:
    """Compute max profit for unlimited transactions."""
    ans = 0
    for i in range(1, len(prices)):
        # If today's price is higher than yesterday's, take the profit
        if prices[i] > prices[i - 1]:
            profit = prices[i] - prices[i - 1]
            ans += profit
            # Debug statement showing each profitable transaction
            print(f"Buy on day {i-1} (${prices[i-1]}) → Sell on day {i} (${prices[i]}) | Profit: {profit}")
    return ans


def main():
    """Demonstration of max_profit2 with sample inputs."""
    print("Example 1: [7,1,5,3,6,4] →", max_profit2([7,1,5,3,6,4]))  # 7
    print("Example 2: [1,2,3,4,5] →", max_profit2([1,2,3,4,5]))      # 4
    print("Example 3: [7,6,4,3,1] →", max_profit2([7,6,4,3,1]))      # 0


if __name__ == "__main__":
    main()
