"""
Can Finish (Course Schedule) — Cycle Detection via Kahn's Algorithm
-------------------------------------------------------------------
Given n courses labeled 0..n-1 and a list of prerequisite edges (u, v) meaning
u -> v (take u before v), determine if all courses can be finished.

Technique (Kahn's Algorithm):
- Build adjacency list and indegree array.
- Initialize a queue with all nodes of indegree 0.
- Repeatedly pop from the queue, decrement indegrees of neighbors, and push new zeros.
- If we process all nodes, the graph is acyclic; otherwise, a cycle exists.

Complexity:
- O(n + m) time, where m = number of edges.
- O(n + m) space for graph, indegrees, and queue.

Run:
    python can_finish.py
"""
from collections import defaultdict, deque
from typing import List, Tuple

def can_finish(n: int, edges: List[Tuple[int, int]]) -> bool:
    g = defaultdict(list)
    indeg = [0] * n
    for u, v in edges:
        g[u].append(v)
        indeg[v] += 1

    q = deque([i for i in range(n) if indeg[i] == 0])
    seen = 0

    while q:
        u = q.popleft()
        seen += 1
        for v in g[u]:
            indeg[v] -= 1
            if indeg[v] == 0:
                q.append(v)

    return seen == n


def main():
    # Example 1: acyclic -> can finish
    n = 4
    edges = [(0,1), (1,2), (2,3)]
    print("Acyclic graph can finish? ->", can_finish(n, edges))

    # Example 2: cycle 1->2->1 -> cannot finish
    n2 = 3
    edges2 = [(1,2), (2,1)]
    print("Cyclic graph can finish? ->", can_finish(n2, edges2))

if __name__ == "__main__":
    main()
