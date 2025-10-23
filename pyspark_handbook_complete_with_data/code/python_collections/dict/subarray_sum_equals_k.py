"""
Dict Interview: Subarray sum equals k using prefix-sum hashmap
Run: python subarray_sum_equals_k.py
"""
def subarray_sum(nums, k):
    pref = 0
    count = {0:1}
    ans = 0
    for x in nums:
        pref += x
        ans += count.get(pref - k, 0)
        count[pref] = count.get(pref, 0) + 1
    return ans

def main():
    print(subarray_sum([1,1,1], 2))

if __name__ == "__main__":
    main()
