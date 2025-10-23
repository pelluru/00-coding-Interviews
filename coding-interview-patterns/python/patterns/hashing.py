from collections import defaultdict
def group_anagrams(strs):
    buckets=defaultdict(list)
    for s in strs:
        key=tuple(sorted(s))
        buckets[key].append(s)
    return list(buckets.values())

def first_unique_char(s):
    from collections import Counter
    c=Counter(s)
    for i,ch in enumerate(s):
        if c[ch]==1: return i
    return -1

def longest_consecutive(nums):
    s=set(nums); best=0
    for x in s:
        if x-1 not in s:
            y=x
            while y in s: y+=1
            best=max(best,y-x)
    return best
