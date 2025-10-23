# Python 3 Collections — Code Repo
Generated on 2025-10-23T12:36:39

This repo contains **3 runnable examples per collection** (built-ins and `collections` module + `heapq`).
Each script has a `main()`.

## Structure
- `src/list/` — 3 examples
- `src/tuple/` — 3 examples
- `src/dict/` — 3 examples
- `src/set/` — 3 examples
- `src/deque/` — 3 examples
- `src/Counter/` — 3 examples
- `src/defaultdict/` — 3 examples
- `src/OrderedDict/` — 3 examples
- `src/namedtuple/` — 3 examples
- `src/ChainMap/` — 3 examples
- `src/heapq/` — 3 examples

## Running
```bash
python src/list/append_extend.py
python src/deque/sliding_window_max.py
# ... etc
```

## Added Interview-Focused Examples (Built-ins)

### list
- rotate_array_right.py — rotate in-place via reversals
- product_except_self.py — prefix/suffix trick
- next_greater_element_circular.py — monotonic stack (list)
- remove_duplicates_sorted_inplace.py — two-pointer write index

### tuple
- merge_intervals_tuples.py — merge intervals using tuples
- unique_pairs_sum_target.py — unique pair sums as tuples
- min_meeting_rooms.py — line sweep with start/end tuples
- grid_paths_blocked_coords.py — DP keyed by (i,j) tuples

### dict
- subarray_sum_equals_k.py — prefix sum hashmap
- isomorphic_strings.py — bijection maps
- word_pattern.py — bijection maps
- group_by_frequency.py — invert frequency map

### set
- longest_consecutive_sequence.py — linear scan using set
- happy_number.py — cycle detection via set
- contains_duplicate_ii.py — sliding window set
- intersection_two_arrays.py — unique intersection
