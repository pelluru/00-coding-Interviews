"""
Minimum Semesters to Finish Courses (Layered Kahn's BFS)
-------------------------------------------------------
You are given n courses and prerequisite edges (u -> v). Taking any number of
"available" courses in parallel per semester, return the minimum number of
semesters needed to finish all courses. If impossible (cycle), return -1.

Technique:
- Kahn's algorithm processed in levels (BFS layers):
  * Each layer corresponds to one semester of courses with indegree 0.
  * After processing a layer, decrement indegrees of neighbors and form next layer.

Complexity:
- O(n + m) time, O(n + m) space.

Run:
    python min_semesters.py
"""
from collections import defaultdict, deque
from typing import List, Tuple

def min_semesters(n: int, edges: List[Tuple[int, int]]) -> int:
    g = defaultdict(list)
    indeg = [0] * n
    for u, v in edges:
        g[u].append(v)
        indeg[v] += 1

    q = deque([i for i in range(n) if indeg[i] == 0])
    sem = 0
    taken = 0

    while q:
        # Process the current "semester" layer
        for _ in range(len(q)):
            u = q.popleft()
            taken += 1
            for v in g[u]:
                indeg[v] -= 1
                if indeg[v] == 0:
                    q.append(v)
        sem += 1

    return sem if taken == n else -1


def main():
    # Example: two semesters
    n = 4
    edges = [(0,2), (1,2), (2,3)]
    print("Min semesters:", min_semesters(n, edges))  # 3 if serial, 3? Let's see: [0,1] -> [2] -> [3] => 3

    # Example: cycle -> -1
    n2 = 2
    edges2 = [(0,1), (1,0)]
    print("Min semesters (cycle):", min_semesters(n2, edges2))

if __name__ == "__main__":
    main()
