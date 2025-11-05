"""
reverse_string_examples.py

This script demonstrates multiple ways to reverse a string in Python:
1. Using reversed() + join
2. Using slicing [::-1]
3. Using a for-loop
4. Using a list and two-pointer swapping
5. Using recursion
6. Using functools.reduce (bonus)

Each function includes clear comments and explanations.
"""

from functools import reduce


def reverse_string(s: str) -> str:
    """
    Reverse string using built-in reversed() and join().
    """
    # reversed(s) returns an iterator that yields characters from end to start
    # ''.join(...) combines the reversed characters into one string
    return ''.join(reversed(s))


def reverse_string_slice(s: str) -> str:
    """
    Reverse string using slicing syntax.
    s[::-1] means start to end in steps of -1 (walk backward).
    """
    return s[::-1]


def reverse_string_loop(s: str) -> str:
    """
    Reverse string manually using a for loop.
    """
    rev = ''
    # iterate through each character in original string
    for ch in s:
        # prepend current character to the front of 'rev'
        rev = ch + rev
    return rev


def reverse_string_list(s: str) -> str:
    """
    Reverse string using two-pointer swap on a list.
    """
    chars = list(s)
    left, right = 0, len(chars) - 1

    while left < right:
        # swap characters
        chars[left], chars[right] = chars[right], chars[left]
        left += 1
        right -= 1

    # join list back into a single string
    return ''.join(chars)


def reverse_string_recursive(s: str) -> str:
    """
    Reverse string using recursion.
    Base case: if string length <= 1, return as is.
    Recursive case: last char + reverse of remaining substring.
    """
    if len(s) <= 1:
        return s
    return s[-1] + reverse_string_recursive(s[:-1])


def reverse_string_reduce(s: str) -> str:
    """
    Reverse string using functional programming with reduce().
    """
    # accumulate reversed string: each step adds current char before accumulator
    return reduce(lambda acc, ch: ch + acc, s, '')


def main():
    """
    Demonstration of all reverse string methods.
    """
    test_str = "python"
    print("Original String:", test_str)
    print("-" * 40)

    print("1. Using reversed() + join:     ", reverse_string(test_str))
    print("2. Using slicing [::-1]:        ", reverse_string_slice(test_str))
    print("3. Using for-loop:              ", reverse_string_loop(test_str))
    print("4. Using list two-pointer swap: ", reverse_string_list(test_str))
    print("5. Using recursion:             ", reverse_string_recursive(test_str))
    print("6. Using reduce():              ", reverse_string_reduce(test_str))
    print("-" * 40)
    print("All methods produce the same result ✅")


if __name__ == "__main__":
    main()
