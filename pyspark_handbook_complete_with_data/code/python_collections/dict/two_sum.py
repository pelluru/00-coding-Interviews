"""
Dict example 3: Two Sum using a hash map
Run: python two_sum.py
"""
def two_sum(nums, target):
    pos = {}
    for i, x in enumerate(nums):
        if target - x in pos:
            return [pos[target - x], i]
        pos[x] = i
    return []

def main():
    print(two_sum([2,7,11,15], 9))

if __name__ == "__main__":
    main()
