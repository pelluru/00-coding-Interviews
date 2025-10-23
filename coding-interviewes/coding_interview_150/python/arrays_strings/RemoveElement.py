"""
Remove Element — Two-Pointer Overwrite
--------------------------------------
Problem:
---------
Given an integer array `nums` and an integer `val`,
remove all occurrences of `val` **in-place**.
The relative order of elements may be changed.

Return the new length `k` after removal,
where the first `k` elements of `nums` contain the elements not equal to `val`.

You must do this using **O(1)** extra space.

Example:
---------
Input : nums = [3,2,2,3], val = 3  
Output: length=2, nums=[2,2]

Explanation:
------------
Remove all `3`s → remaining array = [2,2]

Algorithm (Two-Pointer Overwrite Pattern):
------------------------------------------
1️⃣ Intuition:
   - Use two pointers: one to **read** (`x`) and one to **write** (`w`).
   - We overwrite `nums[w]` with every value not equal to `val`.

2️⃣ Steps:
   - Initialize `w = 0`
   - For each element `x` in nums:
       • If `x != val`, assign `nums[w] = x` and increment `w`.
       • Else, skip the element.
   - Return `w` (the new length).

Why it works:
-------------
Every non-`val` element gets written exactly once at the next open position,
so the first `w` elements of `nums` are the new valid array.

Complexity:
------------
✅ Time : O(n) — single pass through the list  
✅ Space: O(1) — in-place modification  

Edge Cases:
------------
- `nums` empty → returns 0  
- `val` not present → array unchanged  
- All elements = `val` → returns 0
"""

from typing import List

def remove_element(nums: List[int], val: int) -> int:
    """Remove all occurrences of val in-place and return new length."""
    w = 0  # write pointer
    print(f"Initial array: {nums}, val={val}")

    for x in nums:
        if x != val:
            nums[w] = x
            print(f"Keeping {x} at position {w}")
            w += 1
        else:
            print(f"Skipping {x} (equals val)")

    print(f"Final array (up to w={w}): {nums[:w]}")
    return w


def main():
    """Run example test cases for Remove Element."""
    print("\nExample 1:")
    arr = [3,2,2,3]
    k = remove_element(arr, 3)
    print(f"Result length={k}, elements={arr[:k]}")  # [2,2]

    print("\nExample 2:")
    arr = [0,1,2,2,3,0,4,2]
    k = remove_element(arr, 2)
    print(f"Result length={k}, elements={arr[:k]}")  # [0,1,3,0,4]

    print("\nEdge Case 1: No occurrences")
    arr = [1,2,3]
    k = remove_element(arr, 5)
    print(f"Result length={k}, elements={arr[:k]}")  # [1,2,3]

    print("\nEdge Case 2: All elements match val")
    arr = [5,5,5]
    k = remove_element(arr, 5)
    print(f"Result length={k}, elements={arr[:k]}")  # []


if __name__ == "__main__":
    main()
