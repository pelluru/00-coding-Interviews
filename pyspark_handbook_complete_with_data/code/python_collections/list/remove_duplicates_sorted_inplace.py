"""
List Interview: Remove duplicates from sorted array in-place, return new length
Run: python remove_duplicates_sorted_inplace.py
"""
def remove_dups(nums):
    if not nums: return 0
    w = 1
    for r in range(1, len(nums)):
        if nums[r] != nums[w-1]:
            nums[w] = nums[r]
            w += 1
    return w, nums[:w]

def main():
    print(remove_dups([0,0,1,1,1,2,2,3,3,4]))

if __name__ == "__main__":
    main()
