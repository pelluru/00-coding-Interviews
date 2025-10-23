"""
Product of Array Except Self — Prefix * Suffix Technique
--------------------------------------------------------
Problem:
---------
Given an integer array `nums`, return an array `output`
such that `output[i]` is equal to the product of all the elements
of `nums` **except** `nums[i]`.

You must solve it **without using division** and in **O(n)** time.

Example:
---------
Input : [1,2,3,4]
Output: [24,12,8,6]

Explanation:
------------
Index 0 → product of [2,3,4] = 24  
Index 1 → product of [1,3,4] = 12  
Index 2 → product of [1,2,4] = 8  
Index 3 → product of [1,2,3] = 6

Algorithm (Prefix * Suffix):
-----------------------------
1️⃣ **Prefix pass**:
   - Build products of all elements to the **left** of each index.
   - Example: prefix[i] = nums[0] * nums[1] * ... * nums[i-1]

2️⃣ **Suffix pass**:
   - Traverse from right to left, multiply the output array by
     running product of elements **to the right**.

3️⃣ **In-place build**:
   - Use a single output array `out` to store prefix first,
     then multiply by suffix in reverse pass.

Steps:
------
• Initialize out = [1]*n
• pref = 1
• For i in range(n): out[i] = pref; pref *= nums[i]
• suff = 1
• For i in reversed(range(n)): out[i] *= suff; suff *= nums[i]

Why it works:
-------------
At index i:
  prefix product = product of all left-side elements
  suffix product = product of all right-side elements
Multiplying both gives the desired output[i].

Complexity:
------------
✅ Time: O(n) — two passes through array  
✅ Space: O(1) extra — output array not counted as extra space  

Edge Cases:
------------
- Single element → [1]
- Array with zeros → handled correctly (0 in prefix or suffix)
"""

from typing import List

def product_except_self(nums: List[int]) -> List[int]:
    """Return array where each index has product of all other elements."""
    n = len(nums)
    out = [1] * n  # output array initialized to 1s

    # Pass 1: compute prefix products
    pref = 1
    for i in range(n):
        out[i] = pref
        pref *= nums[i]
        print(f"Prefix step {i}: out={out}, pref={pref}")

    # Pass 2: multiply with suffix products
    suff = 1
    for i in range(n - 1, -1, -1):
        out[i] *= suff
        suff *= nums[i]
        print(f"Suffix step {i}: out={out}, suff={suff}")

    return out


def main():
    """Run example test cases for Product of Array Except Self."""
    print("\nExample 1:")
    print("Input : [1,2,3,4]")
    print("Output:", product_except_self([1,2,3,4]))  # [24,12,8,6]

    print("\nExample 2:")
    print("Input : [2,3,4,5]")
    print("Output:", product_except_self([2,3,4,5]))  # [60,40,30,24]

    print("\nEdge Case 1: [0,1,2,3]")
    print("Output:", product_except_self([0,1,2,3]))  # [6,0,0,0]

    print("\nEdge Case 2: [1]")
    print("Output:", product_except_self([1]))        # [1]


if __name__ == "__main__":
    main()
