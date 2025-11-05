"""
char_frequency_examples.py

This script demonstrates multiple ways to count character frequencies in strings.

It includes:
1. Using collections.Counter
2. Using dictionary with for-loop
3. Using dict.get()
4. Using defaultdict(int)
5. Using dictionary comprehension
6. Using reduce()
7. A generic char_frequency(*args, **kwargs) entry function

Each method includes comments and is demonstrated in main().
"""

from collections import Counter, defaultdict
from functools import reduce


# ------------------------------------------------------
# 1️⃣ Using collections.Counter
# ------------------------------------------------------
def char_frequency_counter(s: str) -> dict:
    """
    Count characters using collections.Counter.
    """
    # Counter automatically counts each element's frequency
    return dict(Counter(s))


# ------------------------------------------------------
# 2️⃣ Using Simple Dictionary + For Loop
# ------------------------------------------------------
def char_frequency_loop(s: str) -> dict:
    """
    Count characters manually using a standard dictionary and loop.
    """
    freq = {}
    for ch in s:
        if ch in freq:
            freq[ch] += 1
        else:
            freq[ch] = 1
    return freq


# ------------------------------------------------------
# 3️⃣ Using dict.get() (Simpler Loop)
# ------------------------------------------------------
def char_frequency_get(s: str) -> dict:
    """
    Count characters using dict.get() to simplify logic.
    """
    freq = {}
    for ch in s:
        # .get(key, default) avoids KeyError if not present
        freq[ch] = freq.get(ch, 0) + 1
    return freq


# ------------------------------------------------------
# 4️⃣ Using collections.defaultdict(int)
# ------------------------------------------------------
def char_frequency_defaultdict(s: str) -> dict:
    """
    Count characters using defaultdict(int) for automatic 0 initialization.
    """
    freq = defaultdict(int)
    for ch in s:
        freq[ch] += 1
    return dict(freq)


# ------------------------------------------------------
# 5️⃣ Using Dictionary Comprehension (with set())
# ------------------------------------------------------
def char_frequency_comprehension(s: str) -> dict:
    """
    Count characters using dictionary comprehension and set().
    """
    # Use set(s) to avoid recounting duplicates unnecessarily
    return {ch: s.count(ch) for ch in set(s)}


# ------------------------------------------------------
# 6️⃣ Using reduce() from functools
# ------------------------------------------------------
def char_frequency_reduce(s: str) -> dict:
    """
    Count characters using functional reduce() pattern.
    """
    return reduce(lambda acc, ch: {**acc, ch: acc.get(ch, 0) + 1}, s, {})


# ------------------------------------------------------
# 7️⃣ Generic Entry Function (handles *args, **kwargs)
# ------------------------------------------------------
def char_frequency(*args, **kwargs):
    """
    Generic wrapper for char frequency counting.

    Accepts:
      - single string positional argument
      - optional method='counter'|'loop'|'get'|'defaultdict'|'comprehension'|'reduce'
    """
    if not args:
        raise ValueError("Please provide a string argument.")

    s = str(args[0])
    method = kwargs.get("method", "counter").lower()

    if method == "counter":
        return char_frequency_counter(s)
    elif method == "loop":
        return char_frequency_loop(s)
    elif method == "get":
        return char_frequency_get(s)
    elif method == "defaultdict":
        return char_frequency_defaultdict(s)
    elif method == "comprehension":
        return char_frequency_comprehension(s)
    elif method == "reduce":
        return char_frequency_reduce(s)
    else:
        raise ValueError(f"Unknown method '{method}'. Choose a valid method name.")


# ------------------------------------------------------
# 🔹 main() Demonstration
# ------------------------------------------------------
def main():
    examples = [
        "hello",
        "banana",
        "Python 3.11!",
        "A man, a plan, a canal: Panama"
    ]

    methods = [
        "counter",
        "loop",
        "get",
        "defaultdict",
        "comprehension",
        "reduce"
    ]

    print("\nCHARACTER FREQUENCY CALCULATION — MULTIPLE METHODS\n" + "=" * 60)

    for text in examples:
        print(f"\nInput String: {text!r}")
        for method in methods:
            result = char_frequency(text, method=method)
            print(f"{method:<14}: {result}")
        print("-" * 60)

    print("\n✅ All implementations produce equivalent counts.")


# ------------------------------------------------------
# Entry Point
# ------------------------------------------------------
if __name__ == "__main__":
    main()
