"""
Remove Duplicates from Sorted Array II (Keep ≤ 2 Occurrences)
------------------------------------------------------------
Problem:
---------
Given an integer array `nums` sorted in non-decreasing order,
remove duplicates **in-place** such that each unique element appears
**at most twice**. The relative order of elements should be kept the same.

After removing duplicates, return the new length `k`, where
the first `k` elements of `nums` contain the allowed values.

Example:
---------
Input : [1,1,1,2,2,3]
Output: length=5, nums=[1,1,2,2,3]

Algorithm (Two-Pointer Write Check Against nums[w−2]):
-------------------------------------------------------
1️⃣ Intuition:
   - We can keep each unique number up to two times.
   - The sorted order ensures all duplicates are consecutive.
   - Maintain a **write pointer (w)** to decide where to place the next valid element.

2️⃣ Steps:
   - Initialize `w = 0`
   - For each element `x` in nums:
        • If we have written fewer than 2 elements (`w < 2`), always write.
        • Else, check if `x != nums[w−2]`:
            → means current x differs from the element two positions back,
              so it’s valid to keep another copy.
        • If valid, place it at nums[w] and increment w.

3️⃣ After the loop, `w` is the new length of the array.

Why it works:
-------------
- The check `x != nums[w−2]` ensures that at most two identical values are written.
- As duplicates are grouped, any third occurrence will fail the check and be skipped.

Complexity:
------------
✅ Time : O(n) — single pass through the array  
✅ Space: O(1) — in-place modification  

Edge Cases:
------------
- All unique → array unchanged  
- All identical → only first two retained  
- Empty array → return 0
"""

from typing import List

def remove_dups2(nums: List[int]) -> int:
    """Remove duplicates in-place allowing at most two occurrences."""
    w = 0  # write pointer
    print(f"Initial array: {nums}")

    for x in nums:
        # Allow first two elements automatically, then check condition
        if w < 2 or x != nums[w - 2]:
            nums[w] = x
            w += 1
            print(f"Keeping {x} at position {w-1} → array now: {nums[:w]}")
        else:
            print(f"Skipping {x} (already have two copies)")

    print(f"Final array (up to w={w}): {nums[:w]}")
    return w


def main():
    """Run test cases for Remove Duplicates II."""
    print("\nExample 1:")
    arr = [1,1,1,2,2,3]
    k = remove_dups2(arr)
    print(f"Result length={k}, unique elements={arr[:k]}")  # [1,1,2,2,3]

    print("\nExample 2:")
    arr = [0,0,1,1,1,1,2,3,3]
    k = remove_dups2(arr)
    print(f"Result length={k}, unique elements={arr[:k]}")  # [0,0,1,1,2,3,3]

    print("\nExample 3 (all unique):")
    arr = [1,2,3,4]
    k = remove_dups2(arr)
    print(f"Result length={k}, unique elements={arr[:k]}")  # [1,2,3,4]

    print("\nExample 4 (all identical):")
    arr = [2,2,2,2,2]
    k = remove_dups2(arr)
    print(f"Result length={k}, unique elements={arr[:k]}")  # [2,2]


if __name__ == "__main__":
    main()
