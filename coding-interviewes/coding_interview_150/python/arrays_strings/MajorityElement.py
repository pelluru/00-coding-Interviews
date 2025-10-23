"""
Majority Element — Boyer–Moore Voting Algorithm
-----------------------------------------------
Problem:
---------
Given an integer array `nums`, return the element that appears **more than ⌊n / 2⌋ times**.
You may assume that the majority element always exists.

Goal:
-----
Find the majority element in linear time and constant space.

Algorithm (Boyer–Moore Voting):
--------------------------------
1️⃣ Idea:
   - If we pair each occurrence of the majority element with a different element,
     the majority element will still remain because it occurs more than n/2 times.

2️⃣ Variables:
   - `cand`  → current candidate for majority element
   - `count` → balance counter

3️⃣ Steps:
   - Initialize `cand = None`, `count = 0`
   - For each element x in nums:
       • If count == 0 → choose x as new candidate.
       • If x == cand  → increment count.
       • Else          → decrement count.
   - The final candidate is the majority element.

Why it works:
-------------
The algorithm cancels out non-majority elements pairwise.
Because the majority element count exceeds all others combined,
it will remain as the last surviving candidate.

Complexity:
------------
✅ Time: O(n) — single pass  
✅ Space: O(1) — constant variables

Example:
---------
nums = [2,2,1,1,1,2,2]
Steps:
  count=0, cand=None
  → pick 2 (count=1)
  → 2==cand → count=2
  → 1!=cand → count=1
  → 1!=cand → count=0
  → count=0 → cand=1
  → 2!=cand → count=0 → cand reset
  → cand=2, count=1 → result=2

Result = 2
"""

from typing import List

def majority_element(nums: List[int]) -> int:
    """Return the majority element using Boyer–Moore Voting Algorithm."""
    cand = None   # candidate element
    count = 0     # counter for balancing votes

    for x in nums:
        if count == 0:
            cand = x
            print(f"New candidate selected: {cand}")
        count += 1 if x == cand else -1
        print(f"Element: {x}, Candidate: {cand}, Count: {count}")

    print(f"Final majority candidate: {cand}")
    return cand


def main():
    """Run example test cases for Majority Element."""
    print("\nExample 1:")
    print("Input : [2,2,1,1,1,2,2]")
    print("Output:", majority_element([2,2,1,1,1,2,2]))  # 2

    print("\nExample 2:")
    print("Input : [3,3,4]")
    print("Output:", majority_element([3,3,4]))          # 3

    print("\nExample 3:")
    print("Input : [1]")
    print("Output:", majority_element([1]))              # 1


if __name__ == "__main__":
    main()
