def merge_intervals(intervals):
    intervals.sort(); out=[]
    for s,e in intervals:
        if not out or out[-1][1]<s: out.append([s,e])
        else: out[-1][1]=max(out[-1][1],e)
    return out

def erase_overlap_intervals(intervals):
    if not intervals: return 0
    intervals.sort(key=lambda x:x[1])
    end=intervals[0][1]; keep=1
    for s,e in intervals[1:]:
        if s>=end: keep+=1; end=e
    return len(intervals)-keep

def find_min_arrow_shots(points):
    if not points: return 0
    points.sort(key=lambda x:x[1])
    end=points[0][1]; arrows=1
    for s,e in points[1:]:
        if s>end: arrows+=1; end=e
    return arrows
