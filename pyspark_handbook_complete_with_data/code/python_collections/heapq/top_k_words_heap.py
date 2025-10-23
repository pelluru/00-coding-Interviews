"""
heapq example 3: top-k words with (-freq, word) ordering
Run: python top_k_words_heap.py
"""
import heapq
from collections import Counter

def top_k_words(words, k):
    cnt = Counter(words)
    heap = [(-f, w) for w, f in cnt.items()]
    heapq.heapify(heap)
    return [heapq.heappop(heap)[1] for _ in range(min(k, len(heap)))]

def main():
    print(top_k_words(["i","love","leetcode","i","love","coding"], 2))

if __name__ == "__main__":
    main()
