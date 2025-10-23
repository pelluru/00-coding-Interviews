"""Remove Duplicates I (keep one). Two-pointer read/write.
Time: O(n), Space: O(1)."""
def remove_dups(nums):
    if not nums: return 0
    w=1
    for r in range(1,len(nums)):
        if nums[r]!=nums[w-1]:
            nums[w]=nums[r]; w+=1
    return w

def main():
    arr=[0,0,1,1,1,2,2,3,3,4]; k=remove_dups(arr); print(k, arr[:k])

if __name__ == "__main__":
    main()
