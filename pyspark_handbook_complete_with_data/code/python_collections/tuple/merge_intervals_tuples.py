"""
Tuple Interview: Merge intervals given as tuples
Run: python merge_intervals_tuples.py
"""
def merge(intervals):
    intervals = sorted(intervals, key=lambda x: x[0])
    out = []
    for s,e in intervals:
        if not out or s > out[-1][1]:
            out.append([s,e])
        else:
            out[-1][1] = max(out[-1][1], e)
    return [tuple(x) for x in out]

def main():
    print(merge([(1,3),(2,6),(8,10),(15,18)]))

if __name__ == "__main__":
    main()
