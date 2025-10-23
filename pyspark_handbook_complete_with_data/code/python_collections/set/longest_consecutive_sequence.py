"""
Set Interview: Longest consecutive sequence O(n)
Run: python longest_consecutive_sequence.py
"""
def longest_consecutive(nums):
    S = set(nums)
    best = 0
    for x in S:
        if x-1 not in S:
            y = x
            while y in S:
                y += 1
            best = max(best, y - x)
    return best

def main():
    print(longest_consecutive([100,4,200,1,3,2]))

if __name__ == "__main__":
    main()
