"""
palindrome_examples.py

This script demonstrates multiple ways to check if a given string is a palindrome.
A palindrome reads the same forward and backward (ignoring case and punctuation).

Methods Included:
1. Using slicing [::-1]
2. Using two-pointers
3. Using reversed() iterator
4. Using recursion
5. Using stack (push/pop)
6. Using regular expressions

Each method includes explanations and is showcased in the main() function.
"""

import re
from functools import reduce


# ------------------------------------------------------
# 1️⃣ Using Slicing [::-1]
# ------------------------------------------------------
def is_palindrome(s: str) -> bool:
    """
    Check if a string is a palindrome using slicing.

    Steps:
    - Convert string to lowercase.
    - Keep only alphanumeric characters.
    - Compare string with its reverse using slicing [::-1].
    """
    # Clean the string: lowercase + remove non-alphanumeric
    s = ''.join(c.lower() for c in s if c.isalnum())
    # Compare forward and backward strings
    return s == s[::-1]


# ------------------------------------------------------
# 2️⃣ Two-Pointer Technique
# ------------------------------------------------------
def is_palindrome_two_pointers(s: str) -> bool:
    """
    Check palindrome using two pointers from both ends.
    """
    filtered = [c.lower() for c in s if c.isalnum()]
    left, right = 0, len(filtered) - 1

    # Move inward while comparing characters
    while left < right:
        if filtered[left] != filtered[right]:
            return False
        left += 1
        right -= 1
    return True


# ------------------------------------------------------
# 3️⃣ Using reversed() Built-in
# ------------------------------------------------------
def is_palindrome_reversed(s: str) -> bool:
    """
    Check palindrome using the built-in reversed() iterator.
    """
    s = ''.join(c.lower() for c in s if c.isalnum())
    # Convert both to lists for comparison
    return list(s) == list(reversed(s))


# ------------------------------------------------------
# 4️⃣ Using Recursion
# ------------------------------------------------------
def is_palindrome_recursive(s: str) -> bool:
    """
    Check palindrome using recursion.
    """
    s = ''.join(c.lower() for c in s if c.isalnum())

    def helper(left: int, right: int) -> bool:
        # Base case: pointers have met or crossed
        if left >= right:
            return True
        # Mismatch found
        if s[left] != s[right]:
            return False
        # Move inward recursively
        return helper(left + 1, right - 1)

    return helper(0, len(s) - 1)


# ------------------------------------------------------
# 5️⃣ Using Stack (List Push/Pop)
# ------------------------------------------------------
def is_palindrome_stack(s: str) -> bool:
    """
    Check palindrome using a stack (LIFO principle).
    """
    filtered = [c.lower() for c in s if c.isalnum()]
    stack = []

    # Push each character to stack
    for ch in filtered:
        stack.append(ch)

    # Compare while popping
    for ch in filtered:
        if ch != stack.pop():
            return False

    return True


# ------------------------------------------------------
# 6️⃣ Using Regular Expressions
# ------------------------------------------------------
def is_palindrome_regex(s: str) -> bool:
    """
    Check palindrome using regex for filtering.
    """
    # Keep only alphanumeric, lowercased
    s = re.sub(r'[^A-Za-z0-9]', '', s).lower()
    return s == s[::-1]


# ------------------------------------------------------
# 🔹 main() Demonstration
# ------------------------------------------------------
def main():
    examples = [
        "A man, a plan, a canal: Panama",
        "race a car",
        "No lemon, no melon",
        "Was it a car or a cat I saw?",
        "hello"
    ]

    print("\nPALINDROME CHECKER — MULTIPLE METHODS\n" + "=" * 55)

    for text in examples:
        print(f"\nInput: {text!r}")
        print(f"1. is_palindrome (slicing):       {is_palindrome(text)}")
        print(f"2. two_pointers:                  {is_palindrome_two_pointers(text)}")
        print(f"3. reversed():                    {is_palindrome_reversed(text)}")
        print(f"4. recursion:                     {is_palindrome_recursive(text)}")
        print(f"5. stack:                         {is_palindrome_stack(text)}")
        print(f"6. regex:                         {is_palindrome_regex(text)}")
        print("-" * 55)

    print("\n✅ All implementations produce identical results for valid input strings.")


# ------------------------------------------------------
# Entry point
# ------------------------------------------------------
if __name__ == "__main__":
    main()

