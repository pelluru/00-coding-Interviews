"""Merge Sorted Array (in-place from end)
Algo: i=m-1, j=n-1, k=m+n-1; place larger at k; move.
Time: O(m+n), Space: O(1)."""
def merge(nums1, m, nums2, n):
    i, j, k = m-1, n-1, m+n-1
    while j >= 0:
        if i >= 0 and nums1[i] > nums2[j]:
            nums1[k] = nums1[i]; i -= 1
        else:
            nums1[k] = nums2[j]; j -= 1
        k -= 1

def main():
    a=[1,2,3,0,0,0]; merge(a,3,[2,5,6],3); print(a)

if __name__ == "__main__":
    main()
