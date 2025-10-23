"""
Jump Game — Greedy Maximum Reach
--------------------------------
Problem:
---------
You are given an integer array `nums` where each element represents the maximum jump length
from that position. Starting at index 0, determine if you can reach the **last index**.

Goal:
-----
Return True if you can reach the end, otherwise False.

Example:
---------
Input:  nums = [2,3,1,1,4]
Output: True
Explanation: Jump 1 step to index 1, then 3 steps to the last index.

Input:  nums = [3,2,1,0,4]
Output: False
Explanation: You will always stop at index 3 because nums[3] = 0 and can't move forward.

Algorithm (Greedy Max Reach):
-----------------------------
1️⃣ Initialize a variable `reach` = 0 to track the **farthest index** reachable so far.

2️⃣ Traverse the array using index i and value x = nums[i]:
    • If at any point i > reach, it means we cannot even reach index i — return False.
    • Otherwise, update reach = max(reach, i + x).

3️⃣ After the loop, if we never got stuck, return True.

Why it works:
-------------
- We greedily track the farthest reachable position without simulating every jump.
- If we can always "extend" our reach before getting stuck, we can reach the end.

Complexity:
------------
✅ Time  : O(n) — single pass through the array  
✅ Space : O(1) — only one variable used

Edge Cases:
------------
- Empty list → return False
- [0] → True (already at last index)
- [0,1,2] → False (cannot move forward)
"""

from typing import List

def can_jump(nums: List[int]) -> bool:
    """Return True if we can reach the last index using greedy reach."""
    reach = 0  # farthest index we can reach so far

    for i, jump in enumerate(nums):
        # If current index is beyond our reach, we are stuck
        if i > reach:
            print(f"Stuck at index {i} — cannot reach further (max reach={reach}).")
            return False

        # Update maximum reach from this position
        prev_reach = reach
        reach = max(reach, i + jump)
        print(f"Index {i}: jump={jump}, previous reach={prev_reach}, new reach={reach}")

        # Early exit: if reach already extends beyond last index
        if reach >= len(nums) - 1:
            print(f"Reached or passed the last index at position {i}.")
            return True

    # If loop finishes, check if we covered the last index
    return reach >= len(nums) - 1


def main():
    """Run example test cases for Jump Game."""
    print("\nExample 1:")
    print("Input : [2,3,1,1,4]")
    print("Output:", can_jump([2,3,1,1,4]))  # True

    print("\nExample 2:")
    print("Input : [3,2,1,0,4]")
    print("Output:", can_jump([3,2,1,0,4]))  # False

    print("\nEdge Case 1: [0]")
    print("Output:", can_jump([0]))  # True

    print("\nEdge Case 2: [0,1,2]")
    print("Output:", can_jump([0,1,2]))  # False


if __name__ == "__main__":
    main()
