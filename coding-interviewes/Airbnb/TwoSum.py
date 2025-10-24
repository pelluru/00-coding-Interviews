# Two Sum

# Idea:
# Scan once while storing each number’s index in a hash map. For each num, check if target - num is already in the map.

# Algorithm (One Pass)

# Initialize empty map pos = {} mapping value -> index.

# For each index i and value num:

# need = target - num

# If need is in pos, return [pos[need], i].

# Otherwise store pos[num] = i.

# If no pair found, return [-1, -1] (or raise).

# Complexity:
# Time O(n), Space O(n)

def two_sum(nums, target):
    """
    Return indices of the two numbers such that they add up to target.
    Uses a one-pass hash map for O(n) time.

    Args:
        nums (List[int]): Input array of integers.
        target (int): Desired sum.

    Returns:
        List[int]: Indices [i, j] with nums[i] + nums[j] == target, or [-1, -1] if none.
    """
    pos = {}  # value -> index

    for i, num in enumerate(nums):
        need = target - num  # The complement we are looking for

        # If complement was seen earlier, we found the pair
        if need in pos:
            return [pos[need], i]

        # Otherwise remember current number's index for future complements
        pos[num] = i

    # If no solution exists (depending on problem, you could raise an error)
    return [-1, -1]


def _demo_two_sum():
    nums = [2, 7, 11, 15]
    target = 9
    # Expected indices [0, 1] because nums[0] + nums[1] = 2 + 7 = 9
    print("Two Sum:")
    print("Input:", nums, "target:", target)
    print("Output indices:", two_sum(nums, target))
