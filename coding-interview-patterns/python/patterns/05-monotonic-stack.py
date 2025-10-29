"""

https://www.youtube.com/watch?v=DtJVwbbicjQ

Monotonic Stack — Classic Patterns
==================================

A *monotonic stack* keeps its elements in either non-increasing or non-decreasing
order. It’s perfect when each element’s answer depends on the “nearest”
previous/next element with a larger/smaller value.

Core idea
---------
Scan through the array once and maintain a stack of *indices* such that the
values at those indices are monotonic:

- **Next Greater Element (NGE)** to the right → maintain a **decreasing** stack
  (top is smallest). When a new value `x` is *greater* than the stack’s top
  value, pop indices until the stack top is > x; each popped index’s NGE is `x`.

- **Next Smaller Element (NSE)** to the right → maintain an **increasing** stack
  (top is largest), and pop while top value > x.

Why use indices?
----------------
Indices make it easy to:
  - compute distances/gaps,
  - write answers into a result array at the right positions,
  - check if an index falls outside a sliding structure.

Complexity
----------
All these problems run in **O(n)** time because each index is pushed and popped
at most once. Space is **O(n)** for the stack (worst case).
"""

from typing import List


# ---------------------------------------------------------------------------
# 1) Next Greater Element to the right
# ---------------------------------------------------------------------------

def next_greater(nums: List[int]) -> List[int]:
    """
    For each element, find the first element to its right that is strictly greater.
    If none exists, put -1.

    Algorithm (decreasing stack of indices):
    ----------------------------------------
    - Keep indices in a stack such that nums[stack[0]] > nums[stack[1]] > ...
    - For each index i:
        * While stack not empty and nums[i] > nums[stack[-1]]:
              popped = stack.pop()  -> nums[i] is the NGE for popped
        * Push i to stack.
    - Remaining indices in stack have no greater element to the right → answer -1.

    Example:
      nums = [2, 1, 2, 4, 3]
      ans  = [4, 2, 4, -1, -1]
    """
    n = len(nums)
    ans = [-1] * n
    st: List[int] = []  # stack holds indices; nums[st] strictly decreasing

    for i, x in enumerate(nums):
        # Resolve any indices whose next greater is x
        while st and nums[st[-1]] < x:
            j = st.pop()
            ans[j] = x
        st.append(i)

    return ans


# ---------------------------------------------------------------------------
# 2) Daily Temperatures
# ---------------------------------------------------------------------------

def daily_temperatures(T: List[int]) -> List[int]:
    """
    For each day, how many days until a warmer temperature? If none, 0.

    Algorithm (decreasing stack of indices by temperature):
    -------------------------------------------------------
    - Maintain indices with strictly *decreasing* temps (top is smallest).
    - For each day i with temperature T[i]:
        * While stack not empty and T[i] > T[stack[-1]]:
              j = stack.pop()
              ans[j] = i - j (distance to warmer day)
        * Push i.
    - Unresolved indices have no warmer day → 0.

    Example:
      T = [73,74,75,71,69,72,76,73]
      ans = [1,1,4,2,1,1,0,0]
    """
    n = len(T)
    ans = [0] * n
    st: List[int] = []  # indices; T[st] strictly decreasing

    for i, temp in enumerate(T):
        while st and T[st[-1]] < temp:
            j = st.pop()
            ans[j] = i - j
        st.append(i)

    return ans


# ---------------------------------------------------------------------------
# 3) Largest Rectangle in a Histogram
# ---------------------------------------------------------------------------

def largest_rectangle(heights: List[int]) -> int:
    """
    Given bar heights, return the area of the largest rectangle in the histogram.

    Algorithm (increasing stack of indices):
    ----------------------------------------
    - Maintain indices of bars in **non-decreasing** height order on the stack.
    - Iterate i from 0..n, and process a sentinel height 0 at the end to flush.
    - For each bar i (height h):
        * While stack not empty and heights[stack[-1]] > h:
              H = heights[stack.pop()]  # height of the rectangle
              L = stack[-1] if stack else -1
              width = i - L - 1
              ans = max(ans, H * width)
        * Push i.
    - The trick: when a higher bar ends (we see a smaller h), we finalize all
      rectangles that *used* that taller height as their minimum height.

    Example:
      heights = [2,1,5,6,2,3] → answer = 10 (bars at 5 & 6, width 2)
    """
    st: List[int] = []
    ans = 0
    # Append a sentinel 0 to flush remaining bars
    for i, h in enumerate(heights + [0]):
        while st and heights[st[-1]] > h:
            H = heights[st.pop()]
            L = st[-1] if st else -1
            width = i - L - 1
            ans = max(ans, H * width)
        st.append(i)
    return ans


# ---------------------------------------------------------------------------
# Main: quick demonstrations
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== Monotonic Stack Patterns Demo ===\n")

    # 1) Next Greater Element
    arr = [2, 1, 2, 4, 3]
    nge = next_greater(arr)
    print(f"Next Greater Right for {arr} -> {nge}")
    # Expected: [4, 2, 4, -1, -1]

    # 2) Daily Temperatures
    temps = [73, 74, 75, 71, 69, 72, 76, 73]
    waits = daily_temperatures(temps)
    print(f"Daily Temperatures waits for {temps} -> {waits}")
    # Expected: [1, 1, 4, 2, 1, 1, 0, 0]

    # 3) Largest Rectangle in Histogram
    heights = [2, 1, 5, 6, 2, 3]
    area = largest_rectangle(heights)
    print(f"Largest Rectangle in {heights} -> {area}")
    # Expected: 10
