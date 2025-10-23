"""
H-Index — Sort and Scan Approach
---------------------------------
Problem:
---------
A researcher has published `n` papers. Each paper `i` has `citations[i]` citations.
The H-Index is defined as the maximum value `h` such that the researcher has at least
`h` papers with `h` or more citations each.

Goal:
-----
Return the H-Index given the citations list.

Algorithm (Sort + Linear Scan):
--------------------------------
1️⃣ Sort the list of citations in descending order.

2️⃣ Iterate through the sorted list:
    - For each paper at index i (1-based),
      check if `citations[i-1] >= i`.
    - The largest i that satisfies this condition is the H-Index.

3️⃣ Return that H value.

Why it works:
-------------
Sorting makes it easy to check how many papers have at least `i` citations.
We find the largest number `i` for which this is true.

Example:
---------
citations = [3,0,6,1,5]
Sorted descending = [6,5,3,1,0]
Index (1-based):   1 2 3 4 5
Compare:
 - paper 1: 6 ≥ 1 ✓
 - paper 2: 5 ≥ 2 ✓
 - paper 3: 3 ≥ 3 ✓  → h = 3
 - paper 4: 1 ≥ 4 ✗  → stop
Result: H-Index = 3

Complexity:
------------
✅ Time: O(n log n) — due to sorting  
✅ Space: O(1) — in-place sort

Edge Cases:
------------
- All zeros → h = 0
- Strictly increasing citations → handled
- Empty list → h = 0
"""

from typing import List

def h_index(citations: List[int]) -> int:
    """Compute the H-Index using sort and scan."""
    # Sort citations in descending order
    citations.sort(reverse=True)
    h = 0
    # Iterate through sorted list, 1-based index for H-index logic
    for i, c in enumerate(citations, 1):
        if c >= i:
            h = i   # update H if this paper has at least i citations
            print(f"Paper {i}: citations={c} ≥ {i} → valid (h={h})")
        else:
            print(f"Paper {i}: citations={c} < {i} → stop condition")
            break
    return h


def main():
    """Run demonstration test cases."""
    print("Example 1: [3,0,6,1,5] →", h_index([3,0,6,1,5]))   # 3
    print("Example 2: [1,3,1] →", h_index([1,3,1]))           # 1
    print("Example 3: [0,0,0,0] →", h_index([0,0,0,0]))       # 0
    print("Example 4: [] →", h_index([]))                     # 0
    print("Example 5: [10,8,5,4,3] →", h_index([10,8,5,4,3])) # 4


if __name__ == "__main__":
    main()
