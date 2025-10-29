"""
Topological Order (Kahn's Algorithm)
------------------------------------
Return a valid topological ordering of a DAG. If a cycle exists, return [].

Technique:
- Same as can_finish, but record the order while popping nodes from the queue.
- When the number of popped nodes equals n, the order is valid.

Complexity:
- O(n + m) time, O(n + m) space.

Run:
    python topo_order.py
"""
from collections import defaultdict, deque
from typing import List, Tuple

def topo_order(n: int, edges: List[Tuple[int, int]]) -> List[int]:
    g = defaultdict(list)
    indeg = [0] * n
    for u, v in edges:
        g[u].append(v)
        indeg[v] += 1

    q = deque([i for i in range(n) if indeg[i] == 0])
    order = []

    while q:
        u = q.popleft()
        order.append(u)
        for v in g[u]:
            indeg[v] -= 1
            if indeg[v] == 0:
                q.append(v)

    return order if len(order) == n else []


def main():
    # Example: DAG
    n = 6
    edges = [(5,2), (5,0), (4,0), (4,1), (2,3), (3,1)]
    print("Topo order for DAG:", topo_order(n, edges))

    # Example: cycle -> []
    n2 = 2
    edges2 = [(0,1),(1,0)]
    print("Topo order (cycle):", topo_order(n2, edges2))

if __name__ == "__main__":
    main()
