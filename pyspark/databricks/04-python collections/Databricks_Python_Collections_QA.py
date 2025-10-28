# Databricks notebook source
%md
# Python Collections — Coding Questions & Solutions (Databricks Notebook)
A compact interview-style workbook focused on Python collections: lists, tuples, sets, dicts, `namedtuple`, `Counter`, `defaultdict`, `OrderedDict`, `deque`, `heapq`, `bisect`, `itertools`, and `collections.abc`.

**How to use:** Run top-to-bottom. Each section has a question, constraints, and a clean solution with quick tests.

# COMMAND ----------
# Common imports for multiple tasks
from collections import namedtuple, Counter, defaultdict, OrderedDict, deque
from collections.abc import Mapping, Iterable
import heapq, itertools, bisect

# COMMAND ----------
%md
## Q1) Remove duplicates from a list while preserving order
**Input:** `[3,1,2,3,2,1,4]` → **Output:** `[3,1,2,4]`

**Follow-up:** Do it in O(n) time and O(n) extra space.

# COMMAND ----------
def dedup_preserve_order(nums):
    seen = set()
    out = []
    for x in nums:
        if x not in seen:
            seen.add(x); out.append(x)
    return out

# quick tests
assert dedup_preserve_order([3,1,2,3,2,1,4]) == [3,1,2,4]
assert dedup_preserve_order([]) == []

# COMMAND ----------
%md
## Q2) Group list of tuples by first element (stable)
**Input:** `[("a",1),("b",2),("a",3)]` → **Output:** `{ 'a':[1,3], 'b':[2] }`
Use a dict or `defaultdict(list)`.

# COMMAND ----------
def group_by_key(pairs):
    g = defaultdict(list)
    for k,v in pairs:
        g[k].append(v)
    return dict(g)

assert group_by_key([("a",1),("b",2),("a",3)]) == {"a":[1,3],"b":[2]}

# COMMAND ----------
%md
## Q3) Symmetric difference of unique words from two sentences
Normalize case and strip punctuation; return a sorted list.

# COMMAND ----------
import re
def symdiff_words(s1, s2):
    norm = lambda s: set(re.findall(r"[a-z0-9]+", s.lower()))
    return sorted(norm(s1) ^ norm(s2))

assert symdiff_words("Hello, world!", "hello there world") == ["there"]

# COMMAND ----------
%md
## Q4) Return the k most frequent elements using `Counter.most_common`
**Input:** nums=`[1,1,1,2,2,3]`, k=2 → **Output:** `[1,2]`

# COMMAND ----------
def top_k_frequent(nums, k):
    return [x for x,_ in Counter(nums).most_common(k)]

assert top_k_frequent([1,1,1,2,2,3],2) == [1,2]

# COMMAND ----------
%md
## Q5) Group anagrams using `defaultdict(list)`
**Input:** `['eat','tea','tan','ate','nat','bat']` → groups like `[['eat','tea','ate'], ['tan','nat'], ['bat']]`

# COMMAND ----------
def group_anagrams(words):
    groups = defaultdict(list)
    for w in words:
        key = "".join(sorted(w))
        groups[key].append(w)
    return [sorted(v) for v in groups.values()]

ans = group_anagrams(['eat','tea','tan','ate','nat','bat'])
assert sorted([tuple(x) for x in ans]) == sorted([('ate','eat','tea'),('bat',),('nat','tan')])

# COMMAND ----------
%md
## Q6) Implement a tiny LRU cache with `OrderedDict`
Support `get(key)` and `put(key, value)` with capacity **N**; promote on access.

# COMMAND ----------
class LRUCache:
    def __init__(self, capacity: int):
        self.cap = capacity
        self.od = OrderedDict()
    def get(self, key):
        if key not in self.od: return None
        self.od.move_to_end(key)  # mark as recently used
        return self.od[key]
    def put(self, key, value):
        if key in self.od:
            self.od.move_to_end(key)
        self.od[key] = value
        if len(self.od) > self.cap:
            self.od.popitem(last=False)  # evict LRU

# tests
lru = LRUCache(2)
lru.put("a",1); lru.put("b",2)
assert lru.get("a")==1
lru.put("c",3)  # evicts 'b'
assert "b" not in lru.od and "a" in lru.od and "c" in lru.od

# COMMAND ----------
%md
## Q7) Sliding window maximum with `deque` (O(n))
**Input:** nums=`[1,3,-1,-3,5,3,6,7]`, k=3 → **Output:** `[3,3,5,5,6,7]`

# COMMAND ----------
def max_sliding_window(nums, k):
    dq = deque()  # storing indexes, decreasing by value
    out = []
    for i, x in enumerate(nums):
        # drop indexes outside window
        while dq and dq[0] <= i-k: dq.popleft()
        # maintain decreasing deque
        while dq and nums[dq[-1]] <= x: dq.pop()
        dq.append(i)
        if i >= k-1:
            out.append(nums[dq[0]])
    return out

assert max_sliding_window([1,3,-1,-3,5,3,6,7],3) == [3,3,5,5,6,7]

# COMMAND ----------
%md
## Q8) Use `heapq` to get k smallest and k largest elements
For k largest, use `nlargest`; for k smallest, `nsmallest`. Provide both APIs.

# COMMAND ----------
def k_smallest(nums, k):
    return heapq.nsmallest(k, nums)

def k_largest(nums, k):
    return heapq.nlargest(k, nums)

assert k_smallest([5,1,9,3,7],2) == [1,3]
assert k_largest([5,1,9,3,7],2) == [9,7]

# COMMAND ----------
%md
## Q9) Maintain a sorted list with `bisect` insert
Insert elements into a sorted list while keeping it sorted; return final list.

# COMMAND ----------
def insert_sorted(arr, x):
    i = bisect.bisect_right(arr, x)
    arr.insert(i, x)
    return arr

lst = []
for v in [5,3,8,1,7]:
    insert_sorted(lst, v)
assert lst == [1,3,5,7,8]

# COMMAND ----------
%md
## Q10) Use `namedtuple` for records; sort by multiple keys
Define `User(name, age, score)` and sort by `(-score, age, name)`.

# COMMAND ----------
User = namedtuple("User", "name age score")
users = [User("alice",30,95), User("bob",25,90), User("carl",30,90), User("dina",25,99)]
sorted_users = sorted(users, key=lambda u: (-u.score, u.age, u.name))
assert [u.name for u in sorted_users] == ["dina","alice","bob","carl"]

# COMMAND ----------
%md
## Q11) Merge two dicts with custom conflict resolution
Prefer the larger value; if equal, prefer from `d1`.

# COMMAND ----------
def merge_dicts(d1, d2):
    keys = d1.keys() | d2.keys()
    out = {}
    for k in keys:
        if k in d1 and k in d2:
            out[k] = d1[k] if d1[k] >= d2[k] else d2[k]
        elif k in d1:
            out[k] = d1[k]
        else:
            out[k] = d2[k]
    return out

assert merge_dicts({"a":1,"b":5},{"b":3,"c":2}) == {"a":1,"b":5,"c":2}

# COMMAND ----------
%md
## Q12) Compress consecutive duplicates (run-length) with `itertools.groupby`
**Input:** `AAABBCCCCDAA` → **Output:** `[('A',3),('B',2),('C',4),('D',1),('A',2)]`

# COMMAND ----------
def rle(s: str):
    return [(ch, sum(1 for _ in grp)) for ch, grp in itertools.groupby(s)]

assert rle("AAABBCCCCDAA") == [('A',3),('B',2),('C',4),('D',1),('A',2)]

# COMMAND ----------
%md
## Q13) Nested counts: defaultdict(Counter)
Count product sales per city from `(city, product)` events.

# COMMAND ----------
def nested_counts(events):
    # events: list of (city, product)
    d = defaultdict(Counter)
    for city, product in events:
        d[city][product] += 1
    return {k: dict(v) for k,v in d.items()}

ev = [("ny","laptop"),("sf","mouse"),("ny","mouse"),("sf","mouse"),("ny","laptop")]
out = nested_counts(ev)
assert out["ny"]["laptop"] == 2 and out["sf"]["mouse"] == 2

# COMMAND ----------
%md
## Q14) Verify types with `collections.abc`
Check whether objects satisfy `Mapping` or `Iterable`.

# COMMAND ----------
def is_mapping(x): return isinstance(x, Mapping)
def is_iterable(x): return isinstance(x, Iterable)

assert is_mapping({"a":1}) and not is_mapping([1,2,3])
assert is_iterable((1,2,3)) and is_iterable({"a":1})

# COMMAND ----------
%md
## Q15) Two-sum using a dict for complements
Return **1-based** indexes of two numbers adding to target; or `None`.

# COMMAND ----------
def two_sum(arr, target):
    pos = {}
    for i, x in enumerate(arr):
        need = target - x
        if need in pos:
            return (pos[need]+1, i+1)
        pos[x] = i
    return None

assert two_sum([2,7,11,15],9) == (1,2)
assert two_sum([3,2,4],6) == (2,3)

# COMMAND ----------
%md
## Q16) Top-K frequent words with lexicographic tie-break
Return words sorted by `(-freq, word)`.

# COMMAND ----------
def topk_words(words, k):
    c = Counter(words)
    return [w for w,_ in sorted(c.items(), key=lambda kv: (-kv[1], kv[0]))[:k]]

assert topk_words(["i","love","leetcode","i","love","coding"],2) == ["i","love"]

# COMMAND ----------
%md
## Q17) Median of a data stream with two heaps
Maintain lower max-heap and upper min-heap. Return current median.

# COMMAND ----------
class MedianFinder:
    def __init__(self):
        self.lo = []  # max-heap via negatives
        self.hi = []  # min-heap
    def add(self, x):
        if not self.lo or x <= -self.lo[0]:
            heapq.heappush(self.lo, -x)
        else:
            heapq.heappush(self.hi, x)
        # Rebalance
        if len(self.lo) > len(self.hi) + 1:
            heapq.heappush(self.hi, -heapq.heappop(self.lo))
        elif len(self.hi) > len(self.lo):
            heapq.heappush(self.lo, -heapq.heappop(self.hi))
    def median(self):
        if len(self.lo) > len(self.hi): return float(-self.lo[0])
        return (-self.lo[0] + self.hi[0]) / 2.0

mf = MedianFinder()
for x in [5,2,7,1,9,3]:
    mf.add(x)
assert round(mf.median(),2) == 4.0

# COMMAND ----------
%md
## Q18) Return a read-only view of a dict (mappingproxy)
Expose internal config without allowing mutation from callers.

# COMMAND ----------
from types import MappingProxyType

def readonly_view(d: dict):
    return MappingProxyType(d)

conf = {"a":1}
view = readonly_view(conf)
assert view["a"] == 1
try:
    view["a"] = 2
    assert False, "Should have raised TypeError"
except TypeError:
    pass

# COMMAND ----------
%md
## Q19) Frequency Stack (pop most frequent; tie-break by most recent)
Use `Counter` for freq and a dict `freq → stack` with `deque`.

# COMMAND ----------
class FreqStack:
    def __init__(self):
        self.freq = Counter()
        self.group = defaultdict(deque)  # freq -> stack of values
        self.maxf = 0
    def push(self, x):
        f = self.freq[x] + 1
        self.freq[x] = f
        if f > self.maxf: self.maxf = f
        self.group[f].append(x)
    def pop(self):
        x = self.group[self.maxf].pop()
        self.freq[x] -= 1
        if not self.group[self.maxf]:
            self.maxf -= 1
        return x

fs = FreqStack()
for v in [5,7,5,7,4,5]:
    fs.push(v)
assert fs.pop() == 5
assert fs.pop() == 7
assert fs.pop() == 5
assert fs.pop() == 4

# COMMAND ----------
%md
## Q20) Phone directory allocator (get/release/check)
Use a `set` for available numbers and a `heapq` for recycled order.

# COMMAND ----------
class PhoneDirectory:
    def __init__(self, max_numbers: int):
        self.available = set(range(max_numbers))
        self.recycle = []
    def get(self):
        if self.recycle:
            return heapq.heappop(self.recycle)
        if self.available:
            return self.available.pop()
        return -1
    def check(self, number: int) -> bool:
        return number in self.available or number in set(self.recycle)
    def release(self, number: int):
        if number not in self.available and number not in self.recycle:
            heapq.heappush(self.recycle, number)

pd = PhoneDirectory(3)
x=pd.get(); y=pd.get(); z=pd.get()
assert sorted([x,y,z]) == [0,1,2]
pd.release(x); assert pd.check(x) is True
w=pd.get(); assert w == x

