"""
heapq example 1: kth largest via min-heap of size k
Run: python kth_largest.py
"""
import heapq

def kth_largest(nums, k):
    heap = []
    for x in nums:
        if len(heap) < k:
            heapq.heappush(heap, x)
        elif x > heap[0]:
            heapq.heapreplace(heap, x)
    return heap[0]

def main():
    print(kth_largest([3,2,3,1,2,4,5,5,6], 4))

if __name__ == "__main__":
    main()
