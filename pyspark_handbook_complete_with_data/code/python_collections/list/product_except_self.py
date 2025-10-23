"""
List Interview: Product of Array Except Self (No Division)
==========================================================

Problem:
--------
Given an integer array `nums`, return an array `output` such that:
`output[i]` is equal to the product of all elements of `nums` except `nums[i]`.

⚠️ You cannot use division, and the algorithm must run in O(n) time.

Example:
--------
Input : [1, 2, 3, 4]
Output: [24, 12, 8, 6]

Explanation:
------------
- For index 0 → 2*3*4 = 24
- For index 1 → 1*3*4 = 12
- For index 2 → 1*2*4 = 8
- For index 3 → 1*2*3 = 6

Algorithm (Prefix and Suffix Product Method):
---------------------------------------------
We can calculate products of all elements except self without division by using
two passes — one forward (prefix) and one backward (suffix).

1. **Initialization:**
   - Let `n = len(nums)`
   - Create an output array `out` filled with 1’s → `out = [1]*n`

2. **First pass (Left-to-Right): Compute prefix products**
   - For each index `i`, store product of all elements before `i`.
   - Maintain a running product `pref`.
   - Set `out[i] = pref`, then multiply `pref *= nums[i]`.

   Example:
   nums = [1,2,3,4]
   prefix products: [1, 1, 2, 6] → each `out[i]` is product of elements before i.

3. **Second pass (Right-to-Left): Multiply by suffix products**
   - Maintain another running product `suff` (product of all elements after i).
   - Traverse backward, updating `out[i] *= suff`, then `suff *= nums[i]`.

   Example:
   suffix products (in reverse): [24, 12, 4, 1]
   Multiply with prefix: [1,1,2,6] * [24,12,4,1] → [24,12,8,6]

4. **Return the result array `out`.**

Complexity:
-----------
- Time  : O(n) — two passes over the array.
- Space : O(1) extra — output array not counted as extra space.

Edge Cases:
------------
- Contains zero(s): Works fine (products before and after handled separately).
- Empty list: Return [].
- Single element: Return [1] (by definition).

Dry Run:
--------
nums = [1,2,3,4]
Step 1: prefix pass:
  pref=1
  i=0 → out[0]=1 → pref=1*1=1
  i=1 → out[1]=1 → pref=1*2=2
  i=2 → out[2]=2 → pref=2*3=6
  i=3 → out[3]=6 → pref=6*4=24
  → out after prefix = [1,1,2,6]

Step 2: suffix pass:
  suff=1
  i=3 → out[3]*=1 → 6 → suff=1*4=4
  i=2 → out[2]*=4 → 8 → suff=4*3=12
  i=1 → out[1]*=12 → 12 → suff=12*2=24
  i=0 → out[0]*=24 → 24 → suff=24*1=24
  → out = [24,12,8,6]

Usage:
------
Run: `python product_except_self.py`
"""

from typing import List

def product_except_self(nums: List[int]) -> List[int]:
    """Return array where each element is product of all others except itself."""
    n = len(nums)
    if n == 0:
        return []

    out = [1] * n  # Step 1: initialize output

    # Step 2: prefix pass — compute product of elements before i
    pref = 1
    for i in range(n):
        out[i] = pref
        pref *= nums[i]

    # Step 3: suffix pass — multiply by product of elements after i
    suff = 1
    for i in range(n - 1, -1, -1):
        out[i] *= suff
        suff *= nums[i]

    return out


def main():
    """Demonstrate function with multiple test cases."""
    print(product_except_self([1, 2, 3, 4]))      # [24, 12, 8, 6]
    print(product_except_self([2, 5, 9]))         # [45, 18, 10]
    print(product_except_self([1, 0, 3, 0]))      # [0, 0, 0, 0]
    print(product_except_self([5]))               # [1]
    print(product_except_self([]))                # []

if __name__ == "__main__":
    main()
