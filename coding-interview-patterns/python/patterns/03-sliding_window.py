"""

https://www.youtube.com/watch?v=y2d0VHdvfdc

Sliding Window Algorithms — Classic Patterns
============================================

This module shows how to efficiently process substrings or subarrays
using the *sliding window* technique.

Includes:
1. min_window(s, t)                — smallest substring of s containing all chars of t
2. length_of_longest_substring(s)  — longest substring with all unique chars
3. max_sliding_window(nums, k)     — maximum element in each window of size k
"""

from collections import Counter, deque
from typing import List


# -----------------------------------------------------------------------------
# 1. Minimum Window Substring
# -----------------------------------------------------------------------------
def min_window(s: str, t: str) -> str:
    """
    Find the smallest substring of s that contains all characters of t.

    How it works:
    -------------
    - Maintain a sliding window [l..r] and track character counts.
    - `need` counts chars required from t.
    - `have` counts chars in current window.
    - `formed` = number of chars whose counts in window match those in `need`.
    - Expand right pointer r to include chars until all requirements met.
    - Then, shrink left pointer l to minimize the window while still valid.
    - Keep track of smallest valid window seen so far.

    Complexity:
    ------------
    Time  O(len(s) + len(t))
    Space O(unique_chars_in_t)
    """
    if not s or not t:
        return ""

    need = Counter(t)
    have = Counter()
    req = len(need)   # number of unique chars required
    formed = 0
    l = 0
    best = (float("inf"), None, None)  # (window_size, left, right)

    for r, ch in enumerate(s):
        have[ch] += 1
        # when count of ch matches required count, we satisfy one more requirement
        if ch in need and have[ch] == need[ch]:
            formed += 1

        # try to shrink the window from the left while it's still valid
        while formed == req:
            if r - l + 1 < best[0]:
                best = (r - l + 1, l, r)
            # remove char at left
            c = s[l]
            have[c] -= 1
            if c in need and have[c] < need[c]:
                formed -= 1  # window no longer satisfies this char
            l += 1  # shrink window

    return "" if best[1] is None else s[best[1]: best[2] + 1]


# -----------------------------------------------------------------------------
# 2. Longest Substring Without Repeating Characters
# -----------------------------------------------------------------------------
def length_of_longest_substring(s: str) -> int:
    """
    Return the length of the longest substring without repeating characters.

    How it works:
    -------------
    - Keep track of last seen index of each character in `last`.
    - Maintain a window [l..r] that has unique chars.
    - When you see a repeat of s[r]:
        move l to one position right of the previous occurrence.
    - Update max window length each step.

    Complexity:
    ------------
    Time  O(n)
    Space O(unique_chars)
    """
    last = {}  # maps char -> last index
    l = best = 0

    for r, ch in enumerate(s):
        # If this character was seen inside the current window,
        # move the left pointer past its previous position.
        if ch in last and last[ch] >= l:
            l = last[ch] + 1
        last[ch] = r
        best = max(best, r - l + 1)
    return best


# -----------------------------------------------------------------------------
# 3. Maximum in Sliding Window
# -----------------------------------------------------------------------------
def max_sliding_window(nums: List[int], k: int) -> List[int]:
    """
    Return the maximum value in every contiguous subarray of length k.

    How it works:
    -------------
    - Maintain a decreasing deque of indices (nums[q[i]] is decreasing).
    - For each index i:
        1. Pop indices from back whose values <= nums[i] (not useful anymore).
        2. Add i to the deque.
        3. Remove leftmost if it's outside the window (i - k).
        4. If window is at least size k, record nums[q[0]] (max in window).

    Complexity:
    ------------
    Time  O(n)
    Space O(k)
    """
    q = deque()
    out = []

    for i, x in enumerate(nums):
        # Remove smaller values from the end (they’ll never be max)
        while q and nums[q[-1]] <= x:
            q.pop()

        q.append(i)

        # Remove indices that are out of the current window
        if q[0] <= i - k:
            q.popleft()

        # Once we have a full window, record the max (front of deque)
        if i >= k - 1:
            out.append(nums[q[0]])

    return out


# -----------------------------------------------------------------------------
# Main section for demonstration
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    print("=== Sliding Window Algorithms Demo ===\n")

    # 1. Minimum window substring
    s, t = "ADOBECODEBANC", "ABC"
    result = min_window(s, t)
    print(f"min_window('{s}', '{t}') = '{result}'")  # Expected 'BANC'

    # 2. Longest substring without repeating characters
    s = "abcabcbb"
    length = length_of_longest_substring(s)
    print(f"length_of_longest_substring('{s}') = {length}")  # Expected 3

    # 3. Max in sliding window
    nums, k = [1, 3, -1, -3, 5, 3, 6, 7], 3
    max_vals = max_sliding_window(nums, k)
    print(f"max_sliding_window({nums}, {k}) = {max_vals}")  # Expected [3,3,5,5,6,7]
