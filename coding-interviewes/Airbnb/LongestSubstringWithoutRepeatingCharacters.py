# Longest Substring Without Repeating Characters

# Algorithm Steps (Sliding Window)

# Maintain a window [l..r] with unique characters.

# Use a dictionary last_seen to remember the most recent index of each character.

# Expand the right pointer r one character at a time.

# If a duplicate is found, move l to one position after the last occurrence.

# Update the best (maximum) window length seen so far.

# Complexity

# Time = O(n)

# Space = O(k) (‘k’ = unique character count)


def length_of_longest_substring(s: str) -> int:
    """
    Find the length of the longest substring without repeating characters.
    Uses sliding window with dictionary of last seen indices.
    """
    last_seen = {}  # Maps char -> last seen index
    l = 0           # Left bound of window
    best = 0        # Longest length found so far

    # Step 1: Expand window with right pointer
    for r, ch in enumerate(s):
        # Step 2: If char already seen inside current window
        if ch in last_seen and last_seen[ch] >= l:
            # Move left bound right after previous duplicate
            l = last_seen[ch] + 1

        # Step 3: Record/update this character's latest index
        last_seen[ch] = r

        # Step 4: Update the max window length
        best = max(best, r - l + 1)

    return best


def main():
    print("\n=== Longest Substring Without Repeating Characters ===")
    s1 = "abcabcbb"
    s2 = "bbbbb"
    s3 = "pwwkew"
    print(f"Input: {s1} → Length = {length_of_longest_substring(s1)}")
    print(f"Input: {s2} → Length = {length_of_longest_substring(s2)}")
    print(f"Input: {s3} → Length = {length_of_longest_substring(s3)}")


if __name__ == "__main__":
    main()
