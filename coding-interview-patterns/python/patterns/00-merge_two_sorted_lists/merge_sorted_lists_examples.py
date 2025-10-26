""" 
Merging Two Sorted Lists in Python — Complete Examples
Includes: Two-pointer, heapq.merge, sorted(a+b), and multiple list merge examples.
"""

import heapq

def merge_sorted_lists(a, b):
    """Merge two sorted lists using two-pointer approach."""
    i = j = 0
    merged = []
    while i < len(a) and j < len(b):
        if a[i] < b[j]:
            merged.append(a[i])
            i += 1
        else:
            merged.append(b[j])
            j += 1
    merged.extend(a[i:])
    merged.extend(b[j:])
    return merged


def examples():
    print("=== Two-Pointer Merge (Equal Sizes) ===")
    a = [1, 3, 5, 7]
    b = [2, 4, 6, 8]
    print(merge_sorted_lists(a, b))

    print("\n=== Two-Pointer Merge (Different Sizes) ===")
    a = [1, 4, 9, 15, 20]
    b = [2, 3]
    print(merge_sorted_lists(a, b))

    print("\n=== Edge Cases ===")
    print(merge_sorted_lists([], [1,2,3]))
    print(merge_sorted_lists([1,2,3], []))
    print(merge_sorted_lists([10,20,30], [5,15]))

    print("\n=== heapq.merge() (Two Lists) ===")
    a = [1, 3, 5, 7]
    b = [2, 4, 6, 8]
    print(list(heapq.merge(a, b)))

    print("\n=== sorted(a + b) ===")
    a = [1, 3, 5]
    b = [2, 4, 6]
    print(sorted(a + b))

    print("\n=== heapq.merge() (Multiple Lists) ===")
    list1 = [1, 4, 7]
    list2 = [2, 5, 8]
    list3 = [0, 3, 6]
    print(list(heapq.merge(list1, list2, list3)))


def main():
    print("Merging Two Sorted Lists — Python Examples\n")
    examples()


if __name__ == "__main__":
    main()
