def single_number(nums):
    x=0
    for v in nums: x^=v
    return x

def subsets(nums):
    n=len(nums); out=[]
    for mask in range(1<<n):
        cur=[nums[i] for i in range(n) if mask>>i & 1]
        out.append(cur)
    return out

def count_bits(n):
    ans=[0]*(n+1)
    for i in range(1,n+1): ans[i]=ans[i>>1]+(i&1)
    return ans
