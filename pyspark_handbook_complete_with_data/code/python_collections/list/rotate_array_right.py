"""
List Interview: Rotate array right by k (in-place via reverse)
Run: python rotate_array_right.py
"""
def rotate(nums, k):
    n = len(nums)
    k %= n
    def rev(i,j):
        while i < j:
            nums[i], nums[j] = nums[j], nums[i]
            i += 1; j -= 1
    rev(0, n-1)
    rev(0, k-1)
    rev(k, n-1)
    return nums

def main():
    print(rotate([1,2,3,4,5,6,7], 3))

if __name__ == "__main__":
    main()
