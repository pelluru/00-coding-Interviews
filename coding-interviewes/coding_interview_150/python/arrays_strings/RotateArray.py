"""Rotate Array (right by k) — reversal method.
Time: O(n), Space: O(1)."""
def rotate(nums, k):
    n=len(nums)
    if n==0: return
    k%=n
    def rev(i,j):
        while i<j:
            nums[i],nums[j]=nums[j],nums[i]; i+=1; j-=1
    rev(0,n-1); rev(0,k-1); rev(k,n-1)

def main():
    a=[1,2,3,4,5,6,7]; rotate(a,3); print(a)

if __name__ == "__main__":
    main()
