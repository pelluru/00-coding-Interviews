"""
Gas Station — Greedy Solution
------------------------------
Problem:
---------
There are `n` gas stations arranged in a circle, where the amount of gas at station i is `gas[i]`.
You have a car with an unlimited gas tank, but it costs `cost[i]` to travel from station i to i+1.
You begin with an empty tank and want to know if you can travel around the circuit once.

Goal:
-----
Return the starting gas station index if you can travel around once in the clockwise direction,
otherwise return -1. If there exists a solution, it is guaranteed to be unique.

Algorithm (Greedy Total + Tank Reset):
--------------------------------------
1️⃣ Observation:
   - If the total gas available is less than total cost, it is impossible to complete the circuit.
   - Otherwise, there exists exactly one valid starting point.

2️⃣ Strategy:
   - Keep running totals:
       `total` → net total of gas minus cost across all stations.
       `tank`  → current fuel in tank while simulating the trip.
   - Iterate through all stations:
       diff = gas[i] - cost[i]
       Add diff to total and tank.
       If tank becomes negative → cannot reach next station:
           - Reset tank to 0
           - Choose the next station (i+1) as new starting point.
   - After loop:
       - If total < 0 → return -1 (no solution)
       - Else return `start`.

Why it works:
-------------
Once the tank drops below zero, it means the journey cannot start from any of the previous stations.
Therefore, we skip them and move start to i+1.

Complexity:
-----------
✅ Time: O(n) — single pass through stations  
✅ Space: O(1) — only constant variables used

Edge Cases:
------------
- All costs higher than gas → return -1
- Multiple potential starts → the algorithm ensures only the valid one is returned
- Single station case handled automatically

Example:
---------
gas  = [1,2,3,4,5]
cost = [3,4,5,1,2]
Output: 3 (start at station 3)
"""

from typing import List

def can_complete_circuit(gas: List[int], cost: List[int]) -> int:
    """Return the starting index to complete circuit, or -1 if impossible."""
    total = 0   # net total gas - cost across all stations
    tank = 0    # current gas in the car's tank
    start = 0   # candidate starting station index

    for i in range(len(gas)):
        diff = gas[i] - cost[i]
        total += diff
        tank += diff
        # Debug trace for understanding execution flow
        print(f"Station {i}: Gas={gas[i]}, Cost={cost[i]}, Tank after drive={tank}, Total={total}")
        # If tank is negative, cannot reach next station
        if tank < 0:
            print(f"Cannot reach station {i+1}, resetting start to {i+1}")
            start = i + 1
            tank = 0  # reset current tank for new journey

    # Check feasibility
    return start if total >= 0 else -1


def main():
    """Run example test cases."""
    print("Example 1:")
    print("Gas:  [1,2,3,4,5]")
    print("Cost: [3,4,5,1,2]")
    print("Starting index →", can_complete_circuit([1,2,3,4,5], [3,4,5,1,2]))  # Output: 3

    print("\nExample 2:")
    print("Gas:  [2,3,4]")
    print("Cost: [3,4,3]")
    print("Starting index →", can_complete_circuit([2,3,4], [3,4,3]))  # Output: -1


if __name__ == "__main__":
    main()
