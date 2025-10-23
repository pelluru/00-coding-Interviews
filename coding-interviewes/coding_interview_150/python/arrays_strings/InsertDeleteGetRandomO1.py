"""
Insert Delete GetRandom O(1)
----------------------------
Problem:
---------
Design a data structure that supports the following operations in **average O(1)** time:
  • insert(val) → Inserts an item val into the set if not already present.
  • remove(val) → Removes an item val if present.
  • getRandom() → Returns a random element from the set.

All operations must work in **constant average time**.

Approach (Array + Hash Map Trick):
----------------------------------
We can achieve O(1) for all operations using two structures:
1️⃣ A **dynamic array (`arr`)** to store the elements.
2️⃣ A **hash map (`pos`)** mapping each value → its index in the array.

Algorithm Steps:
----------------
**Insert(val)**:
  • If val already exists in map, return False.
  • Otherwise, append it to the end of array and record its index in the map.

**Remove(val)**:
  • If val not in map, return False.
  • Otherwise:
      - Get its index i.
      - Swap arr[i] with the last element (to maintain array compactness).
      - Update the swapped element’s index in map.
      - Pop the last element and delete val from map.

**getRandom()**:
  • Randomly select an index and return arr[index].
  • Works in O(1) since random.choice() runs in constant time.

Why it works:
-------------
The key trick is that removing from an array’s middle is O(n),
but by swapping with the last element before popping, we keep O(1) removal.

Complexity:
------------
✅ Insert → O(1) average  
✅ Remove → O(1) average  
✅ getRandom → O(1)

Data Structures:
----------------
arr = list of current elements
pos = dict mapping val → index in arr

Example Trace:
--------------
insert(1) → arr=[1], pos={1:0}
insert(2) → arr=[1,2], pos={1:0,2:1}
remove(1) → swap 1 and 2 → arr=[2], pos={2:0}
getRandom() → randomly returns 2

"""

import random
from typing import List, Dict

class RandomizedSet:
    """Implements O(1) insert, remove, and getRandom using list+dict."""

    def __init__(self):
        # Dynamic array to store elements
        self.arr: List[int] = []
        # Map each value to its index in arr
        self.pos: Dict[int, int] = {}

    def insert(self, val: int) -> bool:
        """Insert value if not already present."""
        if val in self.pos:
            print(f"Insert({val}) → already exists.")
            return False
        self.pos[val] = len(self.arr)
        self.arr.append(val)
        print(f"Insert({val}) → inserted at index {self.pos[val]} | arr={self.arr}")
        return True

    def remove(self, val: int) -> bool:
        """Remove value if present."""
        if val not in self.pos:
            print(f"Remove({val}) → not found.")
            return False
        # Index of element to remove
        i = self.pos[val]
        last = self.arr[-1]
        # Swap with last to enable O(1) removal
        self.arr[i] = last
        self.pos[last] = i
        # Pop last element
        self.arr.pop()
        del self.pos[val]
        print(f"Remove({val}) → swapped with last, removed. | arr={self.arr}")
        return True

    def getRandom(self) -> int:
        """Return a random element in O(1)."""
        rand_val = random.choice(self.arr)
        print(f"getRandom() → {rand_val}")
        return rand_val


def main():
    """Demonstrate insert, remove, and getRandom operations."""
    rs = RandomizedSet()

    # Insert elements
    print("\n--- Insert Operations ---")
    rs.insert(1)
    rs.insert(2)
    rs.insert(3)
    rs.insert(1)  # duplicate

    # Random element test
    print("\n--- getRandom() ---")
    rs.getRandom()

    # Remove elements
    print("\n--- Remove Operations ---")
    rs.remove(2)
    rs.remove(5)  # non-existent
    rs.getRandom()
    rs.remove(1)
    rs.getRandom()


if __name__ == "__main__":
    main()

