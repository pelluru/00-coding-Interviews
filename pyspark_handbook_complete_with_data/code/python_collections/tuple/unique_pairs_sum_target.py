"""
Tuple Interview: Unique pairs summing to target using tuples (order-insensitive)
Run: python unique_pairs_sum_target.py
"""
def unique_pairs(nums, target):
    seen = set()
    pairs = set()
    for x in nums:
        if target - x in seen:
            a, b = sorted((x, target-x))
            pairs.add((a,b))
        seen.add(x)
    return sorted(pairs)

def main():
    print(unique_pairs([1,1,2,45,46,46], 47))

if __name__ == "__main__":
    main()
