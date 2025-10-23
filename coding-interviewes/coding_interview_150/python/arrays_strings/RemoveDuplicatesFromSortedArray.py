"""
Remove Duplicates from Sorted Array (Keep One)
----------------------------------------------
Problem:
---------
Given an integer array `nums` sorted in non-decreasing order,
remove the duplicates **in-place** such that each unique element
appears only once. The relative order of elements should be kept the same.

After removing duplicates, return the new length `k`, where the first `k`
elements of `nums` contain the unique values.

The function should modify the array in-place without using extra space.

Example:
---------
Input : [0,0,1,1,1,2,2,3,3,4]
Output: length=5, nums=[0,1,2,3,4]

Algorithm (Two-Pointer Read/Write):
-----------------------------------
1️⃣ Intuition:
   - Since the array is sorted, duplicates are always consecutive.
   - We maintain two pointers:
        `r` → read pointer (scans the array)
        `w` → write pointer (position to place next unique element)

2️⃣ Steps:
   - Initialize `w = 1` (first element is always unique)
   - For each index r from 1 to n−1:
       • If nums[r] != nums[w−1]:
            → new unique found
            → place it at nums[w]
            → increment w
   - After the loop, w represents the count of unique elements.

Why it works:
-------------
Each unique element is written exactly once.  
Duplicates are skipped automatically because they equal the previous written value.

Complexity:
------------
✅ Time : O(n) — single pass through array  
✅ Space: O(1) — in-place modification  

Edge Cases:
------------
- Empty array → return 0
- All unique → return n
- All duplicates → return 1
"""

from typing import List

def remove_dups(nums: List[int]) -> int:
    """Remove duplicates from sorted array in-place and return new length."""
    if not nums:
        print("Empty array provided — returning 0.")
        return 0

    w = 1  # write pointer (index of next unique position)
    print(f"Initial array: {nums}")

    for r in range(1, len(nums)):
        print(f"Comparing nums[r]={nums[r]} with nums[w-1]={nums[w-1]}")
        if nums[r] != nums[w - 1]:
            nums[w] = nums[r]
            print(f"  Unique element {nums[r]} placed at index {w}")
            w += 1
        else:
            print(f"  Duplicate {nums[r]} skipped.")

    print(f"Final array (unique section): {nums[:w]}, length={w}")
    return w


def main():
    """Run sample test cases for remove_dups."""
    print("\nExample 1:")
    arr = [0,0,1,1,1,2,2,3,3,4]
    k = remove_dups(arr)
    print(f"Result length={k}, unique elements={arr[:k]}")

    print("\nExample 2:")
    arr = [1,1,1,1]
    k = remove_dups(arr)
    print(f"Result length={k}, unique elements={arr[:k]}")

    print("\nExample 3:")
    arr = [1,2,3,4]
    k = remove_dups(arr)
    print(f"Result length={k}, unique elements={arr[:k]}")

    print("\nEdge Case: Empty list")
    arr = []
    k = remove_dups(arr)
    print(f"Result length={k}, array={arr}")


if __name__ == "__main__":
    main()
