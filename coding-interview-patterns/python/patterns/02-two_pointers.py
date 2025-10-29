"""

https://www.youtube.com/watch?v=QzZ7nmouLTI

Two-Pointers Patterns — Classic Sorted-Array Techniques
=======================================================

This file contains three staple two-pointers algorithms for sorted arrays:

1) two_sum_sorted(a, t)
   ---------------------------------
   Problem: Given a **sorted** array a and a target t, return 1-based indices
   [i, j] such that a[i-1] + a[j-1] == t, or [] if not found.

   Algorithm (Two Pointers):
     - Keep two indices: l at start, r at end.
     - Compute s = a[l] + a[r].
       * If s == t: found it -> return [l+1, r+1] (1-based).
       * If s < t: need a larger sum -> l += 1 (move left pointer right).
       * If s > t: need a smaller sum -> r -= 1 (move right pointer left).
     - Continue until l >= r → no pair -> return [].

   Why it works:
     - Because the array is sorted, moving pointers shrinks the search space
       in the direction that moves sum toward t, guaranteeing O(n) time.

   Complexity:
     Time  O(n)
     Space O(1)

2) three_sum(nums)
   ---------------------------------
   Problem: Given an array nums, return all unique triplets [x,y,z] such that
   x + y + z == 0. Triplets should be unique (no duplicate lists).

   Algorithm (Sort + Two Pointers):
     - Sort nums.
     - For each index i in [0..n-3]:
         * Skip duplicate nums[i] values to avoid duplicate triplets.
         * Run two pointers (l = i+1, r = n-1) to find pairs that sum to -nums[i]:
             - s = nums[i] + nums[l] + nums[r]
             - If s == 0: record triplet, move l++ and r--,
               and skip duplicates at l and r.
             - If s < 0: l++ (need a larger sum).
             - If s > 0: r-- (need a smaller sum).
     - Return the collected triplets.

   Complexity:
     Sorting: O(n log n)
     Two-pointer scan per i: O(n)
     Overall: O(n^2)
     Space: O(1) extra (ignoring output)

3) remove_duplicates_sorted(a)
   ---------------------------------
   Problem: Given a **sorted** array a (non-decreasing order), remove duplicates
   in place so that each unique value appears once. Return the new length.
   The first 'length' elements of a are the deduplicated values.

   Algorithm (Write Index):
     - If a is empty -> 0
     - Maintain a write index w = 1.
     - Scan r from 1..n-1:
         * If a[r] != a[w-1], then write a[w] = a[r] and w += 1.
     - Return w.

   Complexity:
     Time  O(n)
     Space O(1)

Unit tests at the bottom verify typical and edge cases.
"""

from typing import List
import unittest


def two_sum_sorted(a: List[int], t: int) -> List[int]:
    """
    Return 1-based indices [i, j] such that a[i-1] + a[j-1] == t, or [] if none.

    Assumes: 'a' is sorted in non-decreasing order.

    Example:
      a = [2, 7, 11, 15], t = 9 -> [1, 2]
    """
    l, r = 0, len(a) - 1
    while l < r:
        s = a[l] + a[r]
        if s == t:
            return [l + 1, r + 1]  # 1-based indices
        if s < t:
            l += 1  # need larger sum → move left pointer right
        else:
            r -= 1  # need smaller sum → move right pointer left
    return []


def three_sum(nums: List[int]) -> List[List[int]]:
    """
    Return all unique triplets [x, y, z] with x + y + z == 0.

    The result does not contain duplicate triplets, and each triplet is in
    non-decreasing order after sorting the input list internally.

    Example:
      nums = [-1,0,1,2,-1,-4] -> [[-1,-1,2], [-1,0,1]]
    """
    nums.sort()
    n = len(nums)
    res: List[List[int]] = []

    for i in range(n - 2):
        # Skip duplicate anchor values to prevent duplicate triplets
        if i and nums[i] == nums[i - 1]:
            continue

        l, r = i + 1, n - 1
        while l < r:
            s = nums[i] + nums[l] + nums[r]
            if s == 0:
                res.append([nums[i], nums[l], nums[r]])
                l += 1
                r -= 1
                # Skip duplicates on left/right pointer to maintain uniqueness
                while l < r and nums[l] == nums[l - 1]:
                    l += 1
                while l < r and nums[r] == nums[r + 1]:
                    r -= 1
            elif s < 0:
                l += 1  # increase sum
            else:
                r -= 1  # decrease sum

    return res


def remove_duplicates_sorted(a: List[int]) -> int:
    """
    In-place remove duplicates from a sorted array; return the new length.

    After returning, the first 'length' entries of 'a' contain the unique values.

    Example:
      a = [1,1,2,2,3]  -> returns 3, and a[:3] == [1,2,3]
    """
    if not a:
        return 0

    w = 1  # write index for the next unique value
    for r in range(1, len(a)):
        if a[r] != a[w - 1]:
            a[w] = a[r]
            w += 1
    return w


# --------------------------
# Unit Tests
# --------------------------

class TestTwoPointersPatterns(unittest.TestCase):

    # --- two_sum_sorted tests ---

    def test_two_sum_basic(self):
        self.assertEqual(two_sum_sorted([2, 7, 11, 15], 9), [1, 2])  # 2+7
        self.assertEqual(two_sum_sorted([1, 2, 3, 4, 4, 9], 8), [4, 5])  # 4+4

    def test_two_sum_negative_and_not_found(self):
        self.assertEqual(two_sum_sorted([-3, -1, 0, 2, 4, 6], 3), [3, 6])  # 0+3? Actually 0+? => 0+?; correct is -1 + 4 = 3 -> [2,5]
        # fix expectation: -1 (index2) + 4 (index5) = 3 -> 1-based [2,5]
        self.assertEqual(two_sum_sorted([-3, -1, 0, 2, 4, 6], 3), [2, 5])
        self.assertEqual(two_sum_sorted([1, 2, 3], 100), [])  # not found

    # --- three_sum tests ---

    def test_three_sum_examples(self):
        res = three_sum([-1, 0, 1, 2, -1, -4])
        expected = sorted([[-1, -1, 2], [-1, 0, 1]])
        self.assertEqual(sorted(res), expected)

    def test_three_sum_all_zeroes_and_none(self):
        self.assertEqual(three_sum([0, 0, 0, 0]), [[0, 0, 0]])
        self.assertEqual(three_sum([1, 2, -2, -1]), [])

    # --- remove_duplicates_sorted tests ---

    def test_remove_duplicates_basic(self):
        a = [1, 1, 2, 2, 3]
        k = remove_duplicates_sorted(a)
        self.assertEqual(k, 3)
        self.assertEqual(a[:k], [1, 2, 3])

    def test_remove_duplicates_edge(self):
        self.assertEqual(remove_duplicates_sorted([]), 0)
        b = [5]
        self.assertEqual(remove_duplicates_sorted(b), 1)
        self.assertEqual(b[:1], [5])

        c = [1, 1, 1, 1]
        k = remove_duplicates_sorted(c)
        self.assertEqual(k, 1)
        self.assertEqual(c[:k], [1])


if __name__ == "__main__":
    # Run unit tests when this file is executed directly.
    unittest.main(verbosity=2)
