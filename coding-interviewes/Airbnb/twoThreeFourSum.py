from typing import List

# ===========================================================
# Two Sum, 3Sum, 4Sum — Reusable, Commented Implementations
# ===========================================================

# -----------------------------
# TWO SUM (indices via hashmap)
# -----------------------------
# Algorithm (O(n) time, O(n) space):
# 1) Walk the array once, keeping a dict {value: index}.
# 2) For each number x at index i, check if (target - x) is already in dict.
# 3) If yes, return the pair of indices; otherwise store x->i and continue.
def two_sum_indices(nums: List[int], target: int) -> List[int]:
    seen = {}
    for i, x in enumerate(nums):
        need = target - x
        if need in seen:
            return [seen[need], i]
        seen[x] = i
    return []  # no solution


# ----------------------------------------------
# K-SUM (generic), with 3SUM/4SUM as thin wraps
# ----------------------------------------------
# Core ideas:
# - Sort once, then recursively fix one element and reduce to (k-1)-sum.
# - Base case k==2 uses two pointers to collect unique pairs.
# - Skip duplicates at each level so results are unique.
# - Early pruning: if the smallest or largest possible sums can’t reach target,
#   stop exploring that branch.
#
# Complexity:
# - k-sum worst-case ~ O(n^(k-1)) time; O(k) recursion depth (ignoring output).
def k_sum(nums: List[int], k: int, target: int) -> List[List[int]]:
    nums.sort()  # required for two-pointer and duplicate skipping

    def two_sum(start: int, target2: int) -> List[List[int]]:
        """Return unique pairs from sorted nums[start:] summing to target2."""
        res = []
        l, r = start, len(nums) - 1
        while l < r:
            s = nums[l] + nums[r]
            if s == target2:
                res.append([nums[l], nums[r]])
                # Skip duplicates on both sides
                val_l, val_r = nums[l], nums[r]
                while l < r and nums[l] == val_l:
                    l += 1
                while l < r and nums[r] == val_r:
                    r -= 1
            elif s < target2:
                l += 1
            else:
                r -= 1
        return res

    def helper(start: int, k_now: int, tgt: int) -> List[List[int]]:
        res = []
        n = len(nums)

        # Early pruning using smallest/largest possible sums
        if start >= n:
            return res
        # If the k smallest numbers are too big or the k largest are too small, prune
        smallest = sum(nums[start:start + k_now]) if start + k_now <= n else float('inf')
        largest  = sum(nums[n - k_now:n]) if n - k_now >= start else float('-inf')
        if tgt < smallest or tgt > largest:
            return res

        if k_now == 2:
            return two_sum(start, tgt)

        i = start
        while i < n - k_now + 1:
            # Fix nums[i], solve (k_now-1)-sum on the suffix
            if i > start and nums[i] == nums[i - 1]:
                i += 1
                continue  # skip duplicate fixed value
            for tail in helper(i + 1, k_now - 1, tgt - nums[i]):
                res.append([nums[i]] + tail)
            i += 1
        return res

    return helper(0, k, target)


# ------------------------
# 3SUM: triplets sum to 0
# ------------------------
# Algorithm steps:
# 1) Sort nums.
# 2) Use k_sum with k=3 and target=0 (handles duplicates & two-pointer base).
def three_sum_zero(nums: List[int]) -> List[List[int]]:
    return k_sum(nums, 3, 0)


# -------------------------------
# 4SUM: quadruplets sum to target
# -------------------------------
# Algorithm steps:
# 1) Sort nums.
# 2) Use k_sum with k=4 and the provided target.
def four_sum_target(nums: List[int], target: int) -> List[List[int]]:
    return k_sum(nums, 4, target)


# -------------
# Demo / Tests
# -------------
def main():
    print("=== Two Sum (indices) ===")
    arr = [2, 7, 11, 15]
    print("nums =", arr, "target = 9 ->", two_sum_indices(arr, 9))  # [0,1]

    print("\n=== 3Sum (triplets sum to 0) ===")
    arr3 = [-1, 0, 1, 2, -1, -4]
    print("nums =", arr3, "->", three_sum_zero(arr3))

    print("\n=== 4Sum (quadruplets sum to target) ===")
    arr4 = [1, 0, -1, 0, -2, 2]
    print("nums =", arr4, "target = 0 ->", four_sum_target(arr4, 0))
    print("nums =", arr4, "target = 2 ->", four_sum_target(arr4, 2))

if __name__ == "__main__":
    main()
