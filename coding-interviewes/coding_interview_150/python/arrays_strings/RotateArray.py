"""
Rotate Array (Right by k) — Reversal Method
-------------------------------------------
Problem:
---------
Given an integer array `nums`, rotate the array to the right by `k` steps,
where `k` is non-negative.

Each rotation moves the last element to the front of the array.

Example:
---------
Input : nums = [1,2,3,4,5,6,7], k = 3  
Output: [5,6,7,1,2,3,4]  

Explanation:
------------
Rotate by 1 → [7,1,2,3,4,5,6]  
Rotate by 2 → [6,7,1,2,3,4,5]  
Rotate by 3 → [5,6,7,1,2,3,4]  

Algorithm (3-Step Reversal Technique):
--------------------------------------
1️⃣ **Reverse the entire array**  
   → This moves all elements to the opposite side.  
   Example: [1,2,3,4,5,6,7] → [7,6,5,4,3,2,1]

2️⃣ **Reverse the first k elements**  
   → Places the last k elements (which were rotated to front) in correct order.  
   Example: [7,6,5,4,3,2,1] → [5,6,7,4,3,2,1]

3️⃣ **Reverse the remaining (n − k) elements**  
   → Restores the order of the non-rotated section.  
   Example: [5,6,7,4,3,2,1] → [5,6,7,1,2,3,4]

Why it works:
-------------
By reversing segments strategically, we simulate rotation efficiently
without extra memory or shifting operations.

Complexity:
------------
✅ Time : O(n) — each element swapped at most once  
✅ Space: O(1) — in-place modification  

Edge Cases:
------------
- k = 0 → no rotation  
- k >= n → equivalent to k % n rotations  
- Empty array → return immediately
"""

from typing import List

def rotate(nums: List[int], k: int) -> None:
    """Rotate the array to the right by k steps using the reversal method."""
    n = len(nums)
    if n == 0:
        print("Empty array, nothing to rotate.")
        return

    k %= n  # handle cases where k >= n
    print(f"Rotating array {nums} by {k} steps.")

    # Helper function to reverse elements between indices i and j
    def rev(i: int, j: int) -> None:
        while i < j:
            nums[i], nums[j] = nums[j], nums[i]
            i += 1
            j -= 1

    # Step 1: Reverse the entire array
    rev(0, n - 1)
    print(f"After full reverse: {nums}")

    # Step 2: Reverse first k elements
    rev(0, k - 1)
    print(f"After reversing first {k} elements: {nums}")

    # Step 3: Reverse remaining n-k elements
    rev(k, n - 1)
    print(f"After reversing remaining {n-k} elements: {nums}")

    print(f"Final rotated array: {nums}")


def main():
    """Run example test cases for Rotate Array."""
    print("\nExample 1:")
    arr = [1,2,3,4,5,6,7]
    rotate(arr, 3)
    print("Output:", arr)  # [5,6,7,1,2,3,4]

    print("\nExample 2:")
    arr = [-1,-100,3,99]
    rotate(arr, 2)
    print("Output:", arr)  # [3,99,-1,-100]

    print("\nEdge Case 1: k = 0")
    arr = [1,2,3]
    rotate(arr, 0)
    print("Output:", arr)  # [1,2,3]

    print("\nEdge Case 2: k > n")
    arr = [1,2,3,4]
    rotate(arr, 6)
    print("Output:", arr)  # [3,4,1,2]

    print("\nEdge Case 3: Empty array")
    arr = []
    rotate(arr, 2)
    print("Output:", arr)  # []


if __name__ == "__main__":
    main()
