"""
heapq example 2: merge k sorted lists
Run: python merge_k_sorted.py
"""
import heapq

def merge_k_sorted(lists):
    heap = []
    for li, arr in enumerate(lists):
        if arr:
            heapq.heappush(heap, (arr[0], li, 0))
    out = []
    while heap:
        val, li, idx = heapq.heappop(heap)
        out.append(val)
        if idx + 1 < len(lists[li]):
            nxt = lists[li][idx+1]
            heapq.heappush(heap, (nxt, li, idx+1))
    return out

def main():
    print(merge_k_sorted([[1,4,5],[1,3,4],[2,6]]))

if __name__ == "__main__":
    main()
