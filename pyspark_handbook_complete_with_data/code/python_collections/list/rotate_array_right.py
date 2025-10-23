"""
List Interview: Rotate Array Right by k (In-Place via Reverse)
==============================================================

Problem:
--------
Given an integer array `nums`, rotate the array to the right by `k` steps,
where `k` is non-negative.

Example:
--------
Input : nums = [1,2,3,4,5,6,7], k = 3
Output: [5,6,7,1,2,3,4]

Explanation:
------------
Right rotation by 3 means each element moves 3 positions to the right:
last 3 elements wrap around to the beginning.

Algorithm (In-Place Reversal Method):
-------------------------------------
1. **Idea**:
   - Rotating by k steps to the right is equivalent to:
     - Moving the last k elements to the front, keeping order preserved.

   Instead of using extra arrays, we can achieve this *in-place* with reversals.

2. **Steps**:
   a. Reverse the entire array.
      e.g. [1,2,3,4,5,6,7] → [7,6,5,4,3,2,1]

   b. Reverse the first k elements.
      e.g. reverse([7,6,5]) → [5,6,7,4,3,2,1]

   c. Reverse the remaining n-k elements.
      e.g. reverse([4,3,2,1]) → [5,6,7,1,2,3,4]

   That’s the final rotated array.

3. **Edge Handling**:
   - If k > n, reduce it with `k = k % n`.
   - If array is empty or k == 0, no rotation needed.

4. **Complexity**:
   - Time  : O(n) — each element reversed once.
   - Space : O(1) — done in-place with constant extra space.

Dry Run Example:
----------------
nums = [1,2,3,4,5,6,7], k = 3
Step 1: Reverse all → [7,6,5,4,3,2,1]
Step 2: Reverse first 3 → [5,6,7,4,3,2,1]
Step 3: Reverse rest → [5,6,7,1,2,3,4]
Result: [5,6,7,1,2,3,4]

Usage:
------
Run: `python rotate_array_right.py`
"""

from typing import List

def rotate(nums: List[int], k: int) -> List[int]:
    """Rotate the list nums to the right by k steps in-place."""

    n = len(nums)
    if n == 0:
        return nums

    # Step 1: Normalize k (if k > n, wrap around)
    k %= n
    if k == 0:
        return nums  # no rotation needed

    # Step 2: Define helper function to reverse part of list in-place
    def rev(i: int, j: int) -> None:
        """Reverse elements between indices i and j inclusive."""
        while i < j:
            nums[i], nums[j] = nums[j], nums[i]
            i += 1
            j -= 1

    # Step 3: Reverse entire list
    rev(0, n - 1)
    # Step 4: Reverse first k elements (which were originally last k)
    rev(0, k - 1)
    # Step 5: Reverse remaining n-k elements
    rev(k, n - 1)

    return nums


def main():
    """Demonstrate with example rotations."""
    print(rotate([1,2,3,4,5,6,7], 3))   # [5,6,7,1,2,3,4]
    print(rotate([-1,-100,3,99], 2))    # [3,99,-1,-100]
    print(rotate([1,2], 5))             # [2,1]
    print(rotate([], 1))                # []

if __name__ == "__main__":
    main()
