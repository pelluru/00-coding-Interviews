# Algorithm Steps

# Sort the array to enable two-pointer scanning and easy duplicate skipping.

# Iterate through each element i as a fixed number.

# Use two pointers l (left) and r (right) to find pairs such that
# nums[i] + nums[l] + nums[r] == 0.

# Move pointers inward:

# If sum < 0 → need a larger number ⇒ l += 1

# If sum > 0 → need a smaller number ⇒ r -= 1

# If sum == 0 → record triplet, then skip duplicates on both sides.

# Avoid duplicates by skipping repeated values for both i, l, and r.

# Complexity

# Time = O(n²)

# Space = O(1) (ignoring output list)

def three_sum(nums):
    """
    Find all unique triplets in the array that sum to zero.
    Uses sorting + two-pointer technique.
    """
    nums.sort()          # Step 1: Sort array for ordered traversal
    n = len(nums)
    res = []

    # Step 2: Fix the first element one by one
    for i in range(n):
        if nums[i] > 0:
            # No further triplet can sum to zero (since array is sorted)
            break

        # Skip duplicate 'i' elements to avoid repeated triplets
        if i > 0 and nums[i] == nums[i - 1]:
            continue

        # Step 3: Two-pointer search for pairs that sum to -nums[i]
        target = -nums[i]
        l, r = i + 1, n - 1

        while l < r:
            s = nums[l] + nums[r]
            if s == target:
                # Found one valid triplet
                res.append([nums[i], nums[l], nums[r]])

                # Move both pointers to look for next pair
                l += 1
                r -= 1

                # Skip duplicates for 'l' and 'r'
                while l < r and nums[l] == nums[l - 1]:
                    l += 1
                while l < r and nums[r] == nums[r + 1]:
                    r -= 1

            elif s < target:
                # Need larger sum, move left pointer right
                l += 1
            else:
                # Need smaller sum, move right pointer left
                r -= 1

    return res


def main():
    print("=== 3Sum Problem ===")
    arr = [-1, 0, 1, 2, -1, -4]
    print("Input:", arr)
    result = three_sum(arr)
    print("Triplets that sum to zero:", result)


if __name__ == "__main__":
    main()
