"""
Merge Sorted Array (In-Place from End)
--------------------------------------
Problem:
---------
You are given two integer arrays `nums1` and `nums2`, sorted in non-decreasing order,
and two integers `m` and `n`, representing the number of elements in `nums1` and `nums2` respectively.

`nums1` has enough space (size m + n) to hold additional elements from `nums2`.

Goal:
-----
Merge `nums2` into `nums1` as one sorted array, **in-place**.

Example:
---------
Input :
  nums1 = [1,2,3,0,0,0], m = 3
  nums2 = [2,5,6],       n = 3
Output:
  nums1 = [1,2,2,3,5,6]

Algorithm (Two-Pointer from End):
---------------------------------
1️⃣ Intuition:
   - Since nums1 has extra space at the end, we can fill it from the back to avoid overwriting data.
   - Compare elements from the end of both arrays and place the larger one at the end of nums1.

2️⃣ Pointers:
   - `i = m - 1` → last valid element in nums1
   - `j = n - 1` → last element in nums2
   - `k = m + n - 1` → last index in nums1 (where we place the next largest)

3️⃣ Steps:
   - While `j >= 0` (still have elements in nums2):
        • If i >= 0 and nums1[i] > nums2[j]:
              nums1[k] = nums1[i]; i -= 1
          else:
              nums1[k] = nums2[j]; j -= 1
        • Move `k` one step left (k -= 1)
   - Remaining elements in nums1 are already in correct place.

Why it works:
-------------
We always place the largest element available into the correct end position.
This avoids needing extra space and keeps merging O(1) in space.

Complexity:
------------
✅ Time: O(m + n) — each element considered once  
✅ Space: O(1) — in-place merge

Edge Cases:
------------
- nums2 empty → nums1 unchanged
- nums1 empty → copy nums2 into nums1
- Overlapping ranges handled automatically
"""

from typing import List

def merge(nums1: List[int], m: int, nums2: List[int], n: int) -> None:
    """Merge two sorted arrays in-place into nums1."""
    i = m - 1        # pointer for nums1
    j = n - 1        # pointer for nums2
    k = m + n - 1    # position to fill in nums1

    print(f"Initial State: nums1={nums1}, nums2={nums2}")
    while j >= 0:
        # Debug trace for current pointers
        print(f"Comparing: nums1[{i}]={nums1[i] if i>=0 else 'None'} vs nums2[{j}]={nums2[j]}")
        
        if i >= 0 and nums1[i] > nums2[j]:
            nums1[k] = nums1[i]
            print(f"Placed nums1[{i}]={nums1[i]} at nums1[{k}]")
            i -= 1
        else:
            nums1[k] = nums2[j]
            print(f"Placed nums2[{j}]={nums2[j]} at nums1[{k}]")
            j -= 1
        k -= 1
        print(f"State Update: nums1={nums1}")

    print(f"Final merged array: {nums1}")


def main():
    """Run test cases for merge sorted array."""
    print("\nExample 1:")
    nums1 = [1,2,3,0,0,0]
    merge(nums1, 3, [2,5,6], 3)
    print("Output:", nums1)  # [1,2,2,3,5,6]

    print("\nExample 2:")
    nums1 = [0]
    merge(nums1, 0, [1], 1)
    print("Output:", nums1)  # [1]

    print("\nExample 3:")
    nums1 = [2,0]
    merge(nums1, 1, [1], 1)
    print("Output:", nums1)  # [1,2]


if __name__ == "__main__":
    main()
