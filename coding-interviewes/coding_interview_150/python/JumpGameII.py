
"""Jump Game II — greedy windows. Time: O(n), Space: O(1)"""
def jump(nums):
    jumps=0; cur_end=0; far=0
    for i in range(len(nums)-1):
        far=max(far,i+nums[i])
        if i==cur_end:
            jumps+=1; cur_end=far
    return jumps
def main():
    print(jump([2,3,1,1,4]))
    print(jump([2,3,0,1,4]))
if __name__=='__main__': main()
