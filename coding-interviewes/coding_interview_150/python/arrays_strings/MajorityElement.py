"""Majority Element — Boyer-Moore Voting.
Time: O(n), Space: O(1)."""
def majority_element(nums):
    cand=None; count=0
    for x in nums:
        if count==0: cand=x
        count += 1 if x==cand else -1
    return cand

def main():
    print(majority_element([2,2,1,1,1,2,2]))

if __name__ == "__main__":
    main()
