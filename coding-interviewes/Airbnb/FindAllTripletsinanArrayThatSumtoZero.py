from typing import List, Iterable, Tuple

# ===========================================================
# Algorithm Overview (3Sum using Two-Pointer and Sorting)
# ===========================================================
# 1. Sort the array to allow ordered traversal and duplicate skipping.
# 2. Iterate each index i as the "fixed" element.
# 3. For each i, find all pairs (l, r) such that nums[i] + nums[l] + nums[r] == 0.
#    - Use the two-pointer approach to efficiently find pairs.
#    - Move pointers inward depending on the sum.
# 4. Skip duplicate values for i, l, and r to avoid duplicate triplets.
# 5. Complexity:
#    - Time: O(n²)  (Sorting + nested two-pointer scan)
#    - Space: O(1)  (ignoring output)
# ===========================================================


def _skip_dups(nums: List[int], i: int, step: int, bound: int) -> int:
    """
    Helper to move index `i` in direction `step` (+1 or -1) 
    while skipping over duplicate values within bounds.
    Example:
        nums = [-1, -1, 0, 1], i=0, step=+1 → skips to index 2
    """
    j = i + step
    while 0 <= j < bound and nums[j] == nums[i]:
        j += step
    return j


def two_sum_sorted(nums: List[int], l: int, r: int, target: int) -> Iterable[Tuple[int, int]]:
    """
    Two-pointer search on a sorted list between indices l and r
    to find all pairs (l, r) where nums[l] + nums[r] == target.

    Steps:
    1. Initialize left = l, right = r
    2. While left < right:
        - If sum == target: yield pair and skip duplicates
        - If sum < target: move left pointer rightward
        - If sum > target: move right pointer leftward
    """
    while l < r:
        s = nums[l] + nums[r]
        if s == target:
            # Found a valid pair
            yield (l, r)
            # Skip duplicates for both pointers
            l = _skip_dups(nums, l, +1, r)
            r = _skip_dups(nums, r, -1, l - 1)
        elif s < target:
            l += 1
        else:
            r -= 1


def three_sum(nums: List[int]) -> List[List[int]]:
    """
    Finds all unique triplets [a, b, c] in nums such that a + b + c == 0.

    Algorithm:
    1. Sort array
    2. Iterate each element i
    3. Use two_sum_sorted to find complement pairs
    4. Skip duplicates and collect results
    """
    nums.sort()
    n = len(nums)
    result: List[List[int]] = []

    # Iterate each number as the fixed element
    for i in range(n - 2):
        # Early termination (since all numbers after are positive)
        if nums[i] > 0:
            break

        # Skip duplicates for the fixed element
        if i > 0 and nums[i] == nums[i - 1]:
            continue

        # Target is the negation of the fixed element
        target = -nums[i]

        # Find pairs (l, r) such that nums[l] + nums[r] == target
        for l, r in two_sum_sorted(nums, i + 1, n - 1, target):
            result.append([nums[i], nums[l], nums[r]])

    return result


def main():
    """
    Driver function to test the 3Sum algorithm.
    """
    print("=== 3Sum Problem (Reusable Functions) ===")

    # Example test cases
    test_cases = [
        [-1, 0, 1, 2, -1, -4],
        [0, 0, 0, 0],
        [-2, 0, 1, 1, 2, -1, -4, 2, -1]
    ]

    for arr in test_cases:
        print(f"\nInput Array: {arr}")
        triplets = three_sum(arr)
        print(f"Triplets that sum to zero: {triplets}")


# Standard main entry point
if __name__ == "__main__":
    main()
