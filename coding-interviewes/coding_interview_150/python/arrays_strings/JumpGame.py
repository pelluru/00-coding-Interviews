
"""Jump Game — greedy max reach. Time: O(n), Space: O(1)"""
def can_jump(nums):
    reach=0
    for i,x in enumerate(nums):
        if i>reach: return False
        reach=max(reach,i+x)
    return True
def main():
    print(can_jump([2,3,1,1,4]))
    print(can_jump([3,2,1,0,4]))
if __name__=='__main__': main()
