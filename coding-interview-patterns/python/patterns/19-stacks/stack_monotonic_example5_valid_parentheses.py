#!/usr/bin/env python3
"""Example 5: Valid Parentheses
Check if the input string has valid bracket nesting using a stack.

Algorithm:
- Push opening brackets. On closing, check if top matches.
- Valid if stack empty at end and all matches correct.
Time: O(n), Space: O(n).
"""
from typing import List

def is_valid_parentheses(s: str) -> bool:
    pairs = {')': '(', ']': '[', '}': '{'}
    st: List[str] = []
    for ch in s:
        if ch in '([{':
            st.append(ch)
        elif ch in pairs:
            if not st or st[-1] != pairs[ch]:
                return False
            st.pop()
        else:
            # ignore non-bracket chars, or return False if strict
            pass
    return not st

if __name__ == "__main__":
    print("Example 5 — Valid Parentheses:", is_valid_parentheses("()[]{}"), is_valid_parentheses("(]"))  # True False
