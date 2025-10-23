"""Remove Duplicates II (keep <=2). Check against nums[w-2].
Time: O(n), Space: O(1)."""
def remove_dups2(nums):
    w=0
    for x in nums:
        if w<2 or x!=nums[w-2]:
            nums[w]=x; w+=1
    return w

def main():
    arr=[1,1,1,2,2,3]; k=remove_dups2(arr); print(k, arr[:k])

if __name__ == "__main__":
    main()
