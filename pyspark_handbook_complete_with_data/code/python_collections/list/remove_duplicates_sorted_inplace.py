"""
List Interview: Remove Duplicates from Sorted Array (In-Place)
==============================================================

Problem:
--------
Given a sorted integer array `nums`, remove duplicates *in-place* so that 
each unique element appears only once. The relative order of elements 
should be preserved. Return the new length and the modified array (first `w` elements).

Example:
--------
Input : [0,0,1,1,1,2,2,3,3,4]
Output: (5, [0,1,2,3,4])

Why it works:
-------------
The array is sorted, so duplicates always appear *consecutively*.  
We can use a **two-pointer technique**:

Algorithm Steps:
----------------
1. Handle the edge case:
   - If `nums` is empty, return 0 immediately.

2. Initialize a **write pointer `w`** at index 1.
   - This points to the next position to place the next unique value.

3. Iterate with **read pointer `r`** from index 1 to end of array:
   - Compare current number `nums[r]` with previous unique number `nums[w-1]`.
   - If they differ, it's a new unique number:
        → Assign `nums[w] = nums[r]`
        → Increment `w` by 1

4. After the loop, `w` will be the count of unique elements.
   - The first `w` elements of `nums` contain the deduplicated array.

5. Return `(w, nums[:w])`.

Complexity:
-----------
- Time  : O(n) — single traversal.
- Space : O(1) — done in-place (constant extra space).

Dry Run Example:
----------------
nums = [0,0,1,1,1,2,2,3,3,4]
w=1

r=1: nums[1]=0 == nums[w-1]=0 → skip
r=2: nums[2]=1 != nums[w-1]=0 → nums[w]=1 → w=2
r=3: nums[3]=1 == nums[1]=1 → skip
r=4: nums[4]=1 == nums[1]=1 → skip
r=5: nums[5]=2 != nums[1]=1 → nums[w]=2 → w=3
r=6: nums[6]=2 == nums[2]=2 → skip
r=7: nums[7]=3 != nums[2]=2 → nums[w]=3 → w=4
r=8: nums[8]=3 == nums[3]=3 → skip
r=9: nums[9]=4 != nums[3]=3 → nums[w]=4 → w=5

Final: w=5, nums[:w]=[0,1,2,3,4]

Usage:
------
Run: `python remove_duplicates_sorted_inplace.py`
"""

from typing import List, Tuple

def remove_dups(nums: List[int]) -> Tuple[int, List[int]]:
    """Remove duplicates from sorted list in-place and return (length, new_list)."""
    
    # Step 1: Handle empty input
    if not nums:
        return 0, []

    # Step 2: Initialize write pointer (next position for unique number)
    w = 1

    # Step 3: Iterate with read pointer from 1..n-1
    for r in range(1, len(nums)):
        # Step 4: If current number is different from last unique one
        if nums[r] != nums[w - 1]:
            nums[w] = nums[r]  # Place new unique value
            w += 1             # Move write pointer forward

    # Step 5: Return count and sliced array of unique elements
    return w, nums[:w]


def main():
    """Demonstrate with sample inputs."""
    print(remove_dups([0,0,1,1,1,2,2,3,3,4]))  # Expected: (5, [0,1,2,3,4])
    print(remove_dups([1,1,2]))                # Expected: (2, [1,2])
    print(remove_dups([]))                     # Expected: (0, [])
    print(remove_dups([1,1,1]))                # Expected: (1, [1])

if __name__ == "__main__":
    main()
