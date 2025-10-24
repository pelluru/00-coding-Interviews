# Group Anagrams

# Idea 1 (Counting Key — Preferred):
# Use a 26-length frequency tuple as the key for each lowercase word. All anagrams share identical letter counts.

# Algorithm (Counting)

# Initialize dict groups = {} mapping count_tuple -> list_of_words.

# For each word w:

# Build an array count[26] where count[c] is frequency of letter c.

# Convert count to tuple key and append w to groups[key].

# Return all groups.values().

# Complexity:
# Let n be #words and L be average word length.
# Time O(n * L); Space O(n * L) for output + keys.

# Key Concepts Recap

# Hashing / Dictionary: Constant-time average lookups (in, dict[key]) to match complements (Two Sum) or bucket by signature (Group Anagrams).

# Frequency Count: A stable, order-independent signature for anagrams without sorting.

# Space–Time Tradeoff: Extra memory for maps buys linear-time passes.


from collections import defaultdict

def group_anagrams(words):
    """
    Group words that are anagrams using a 26-length frequency count as the key.
    This avoids O(L log L) sorting per word.

    Args:
        words (List[str]): Input list of strings (assumed lowercase a-z for counting approach).

    Returns:
        List[List[str]]: Grouped anagrams.
    """
    groups = defaultdict(list)  # key (tuple of 26 counts) -> list of anagrams

    for w in words:
        # Build fixed-size frequency vector for 'a'..'z'
        count = [0] * 26
        for ch in w:
            # Map character to index 0..25
            idx = ord(ch) - ord('a')
            # If inputs may have uppercase or non-letters, you can pre-normalize or switch to sorted-key method
            if 0 <= idx < 26:
                count[idx] += 1
            else:
                # Fallback: for non a-z, you can switch to a generic map or use sorting-based key
                # For simplicity, include the raw char in an extension of the key
                # But here we assume lowercase letters as per common interview variant.
                pass

        key = tuple(count)         # Tuples are hashable and can be used as dict keys
        groups[key].append(w)      # Append the word into its anagram bucket

    return list(groups.values())


def group_anagrams_sorted(words):
    """
    Alternative method: use the sorted string as the key.
    Simpler, but O(L log L) per word due to sorting.
    """
    groups = defaultdict(list)
    for w in words:
        key = ''.join(sorted(w))   # Sorting letters produces identical keys for anagrams
        groups[key].append(w)
    return list(groups.values())


def _demo_group_anagrams():
    words = ["eat", "tea", "tan", "ate", "nat", "bat"]
    print("\nGroup Anagrams (count-key):")
    print("Input:", words)
    print("Output:", group_anagrams(words))

    print("\nGroup Anagrams (sorted-key):")
    print("Input:", words)
    print("Output:", group_anagrams_sorted(words))
