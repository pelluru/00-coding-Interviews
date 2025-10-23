"""
Set Interview: Contains duplicate within k distance (sliding window set)
Run: python contains_duplicate_ii.py
"""
def contains_nearby_duplicate(nums, k):
    window = set()
    l = 0
    for r, x in enumerate(nums):
        if r - l > k:
            window.discard(nums[l]); l += 1
        if x in window: return True
        window.add(x)
    return False

def main():
    print(contains_nearby_duplicate([1,2,3,1], 3), contains_nearby_duplicate([1,0,1,1], 1))

if __name__ == "__main__":
    main()
