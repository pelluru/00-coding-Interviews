
"""Product of Array Except Self — prefix*suffix. Time: O(n)"""
def product_except_self(nums):
    n=len(nums); out=[1]*n; pref=1
    for i in range(n): out[i]=pref; pref*=nums[i]
    suff=1
    for i in range(n-1,-1,-1): out[i]*=suff; suff*=nums[i]
    return out
def main():
    print(product_except_self([1,2,3,4]))
if __name__=='__main__': main()
