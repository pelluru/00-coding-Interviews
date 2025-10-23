"""Remove Element — two-pointer overwrite.
Time: O(n), Space: O(1)."""
def remove_element(nums, val):
    w=0
    for x in nums:
        if x!=val:
            nums[w]=x; w+=1
    return w

def main():
    arr=[3,2,2,3]; k=remove_element(arr,3); print(k, arr[:k])

if __name__ == "__main__":
    main()
