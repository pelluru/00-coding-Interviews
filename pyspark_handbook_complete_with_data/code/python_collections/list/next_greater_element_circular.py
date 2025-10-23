"""
List Interview: Next Greater Element (Circular Array) using Stack (List)
=======================================================================

Problem:
--------
Given a *circular* array `nums`, for every index `i`, find the next greater element
when moving clockwise. If no such element exists, return -1 for that index.

For example:
Input:  [1, 2, 1]
Output: [2, -1, 2]

Algorithm Steps:
----------------
1. Initialize:
   - `n` = length of array
   - `ans` = array of length `n` filled with -1 (default "no greater element")
   - `st` = empty stack to store *indices* of elements waiting for a greater value.

2. Iterate `i` from 0 to `2n - 1` (to simulate circular traversal):
   - Let `x = nums[i % n]` (wrap around using modulo)
   - While the stack is not empty and the current element `x` is greater than
     `nums[st[-1]]` (top of the stack):
        → Pop the index from the stack.
        → The current `x` is the next greater element for that popped index.
        → Assign `ans[popped_index] = x`.

   - Only push index `i` onto the stack during the *first pass* (`i < n`),
     to ensure each element is pushed once and processed once.

3. After two passes, all next greater elements will be found, or remain -1 if none exists.

4. Return `ans`.

Complexity:
-----------
Time  : O(n) — each element is pushed and popped at most once.
Space : O(n) — for the stack and result array.

Edge Cases:
------------
- Empty input → returns [].
- All decreasing elements → only wrap-around may resolve some, or remain -1.
- All equal elements → all -1.

Dry Run Example:
----------------
nums = [1, 2, 1], n=3

i=0, x=1 → stack=[] → push(0)
i=1, x=2 → nums[stack[-1]]=1 < 2 → pop(0), ans[0]=2 → push(1)
i=2, x=1 → nums[1]=2 < 1? no → push(2)
i=3, x=1 → skip push (i>=n)
i=4, x=2 → nums[2]=1 < 2 → pop(2), ans[2]=2
           nums[1]=2 < 2? no
i=5, x=1 → nothing pops

Final ans = [2, -1, 2]
"""

from typing import List

def next_greater_elements(nums: List[int]) -> List[int]:
    n = len(nums)
    if n == 0:
        return []

    ans = [-1] * n        # Initialize with -1 (no greater element)
    st = []               # Stack to hold indices of elements waiting for NGE

    # Iterate twice to simulate circular array
    for i in range(2 * n):
        x = nums[i % n]   # Current element (wrap around using modulo)

        # Resolve elements in stack where current x is the next greater
        while st and nums[st[-1]] < x:
            prev_idx = st.pop()
            ans[prev_idx] = x

        # Only push index in the first traversal
        if i < n:
            st.append(i)

    return ans


def main():
    print("Example 1:", next_greater_elements([1, 2, 1]))      # [2, -1, 2]
    print("Example 2:", next_greater_elements([1, 2, 3, 4, 3])) # [2,3,4,-1,4]
    print("Example 3:", next_greater_elements([5,4,3,2,1]))     # [-1,5,5,5,5]
    print("Example 4:", next_greater_elements([]))              # []

if __name__ == "__main__":
    main()
