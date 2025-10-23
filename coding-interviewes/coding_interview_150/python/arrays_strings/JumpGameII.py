"""
Jump Game II — Greedy Minimum Jumps
-----------------------------------
Problem:
---------
Given an integer array `nums` where each element represents your maximum jump length at that position,
return the minimum number of jumps needed to reach the **last index**.

You can always reach the end of the array.

Example:
---------
Input : nums = [2,3,1,1,4]
Output: 2
Explanation:
    Jump 1 → from index 0 to 1 (value 3)
    Jump 2 → from index 1 to 4 (last index)

Algorithm (Greedy — Window Expansion):
--------------------------------------
1️⃣ Intuition:
   - We can think of the array as **layers** (or windows) of reachable indices.
   - Every time we finish scanning the current layer, we make one jump to move to the next layer.

2️⃣ Variables:
   - `jumps`   → number of jumps taken so far
   - `cur_end` → the end index of the current layer (max index reachable with current jumps)
   - `farthest`→ the farthest index we can reach in the next layer

3️⃣ Steps:
   - Iterate through nums (except the last index):
       - Update `farthest = max(farthest, i + nums[i])`
       - When we reach the end of the current layer (i == cur_end):
           - Increment jumps
           - Move to the next layer by setting `cur_end = farthest`
   - Return total `jumps`.

Why it works:
-------------
We always take the minimum number of jumps by extending the reach as far as possible
before committing to the next jump — hence greedy optimality.

Complexity:
------------
✅ Time: O(n) — single pass through array  
✅ Space: O(1) — only a few variables used

Edge Cases:
------------
- Single element (already at destination) → 0 jumps
- All 1s → number of jumps = n−1
"""

from typing import List

def jump(nums: List[int]) -> int:
    """Return the minimum number of jumps to reach the end."""
    if len(nums) <= 1:
        return 0  # no jump needed if array has 1 or 0 elements

    jumps = 0        # total jumps taken
    cur_end = 0      # end of current reachable window
    farthest = 0     # farthest index reachable in next jump window

    for i in range(len(nums) - 1):
        # Update farthest reachable index
        farthest = max(farthest, i + nums[i])
        print(f"Index {i}: nums[i]={nums[i]}, farthest={farthest}, current window end={cur_end}")

        # When we reach the end of current jump window
        if i == cur_end:
            jumps += 1
            cur_end = farthest
            print(f"  Jump {jumps} → New window end = {cur_end}")

        # Early exit if we've already reached or passed the end
        if cur_end >= len(nums) - 1:
            print(f"Reached the end at index {i}, total jumps = {jumps}")
            break

    return jumps


def main():
    """Run example test cases for Jump Game II."""
    print("\nExample 1:")
    print("Input : [2,3,1,1,4]")
    print("Output:", jump([2,3,1,1,4]))  # 2

    print("\nExample 2:")
    print("Input : [2,3,0,1,4]")
    print("Output:", jump([2,3,0,1,4]))  # 2

    print("\nEdge Case 1: [1]")
    print("Output:", jump([1]))  # 0

    print("\nEdge Case 2: [1,1,1,1]")
    print("Output:", jump([1,1,1,1]))  # 3


if __name__ == "__main__":
    main()
