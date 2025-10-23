"""
Dict Interview: Group elements by frequency using dict
Run: python group_by_frequency.py
"""
def group_by_freq(nums):
    freq = {}
    for x in nums:
        freq[x] = freq.get(x, 0) + 1
    groups = {}
    for x, f in freq.items():
        groups.setdefault(f, []).append(x)
    return {k: sorted(v) for k, v in groups.items()}

def main():
    print(group_by_freq([1,1,2,2,2,3]))

if __name__ == "__main__":
    main()
