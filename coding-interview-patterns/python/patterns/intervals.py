import heapq as hq
def min_meeting_rooms(intervals):
    intervals.sort(key=lambda x:x[0])
    heap=[]
    for s,e in intervals:
        if heap and heap[0]<=s: hq.heapreplace(heap,e)
        else: hq.heappush(heap,e)
    return len(heap)

def employee_free_time(schedules):
    # Flatten and merge, then compute gaps. Simpler approach for demo.
    intervals=[iv for person in schedules for iv in person]
    intervals.sort()
    merged=[]
    for s,e in intervals:
        if not merged or merged[-1][1]<s: merged.append([s,e])
        else: merged[-1][1]=max(merged[-1][1],e)
    free=[]
    for i in range(1,len(merged)):
        if merged[i-1][1] < merged[i][0]: free.append([merged[i-1][1], merged[i][0]])
    return free

def erase_overlap_intervals(intervals):
    if not intervals: return 0
    intervals.sort(key=lambda x:x[1])
    end=intervals[0][1]; keep=1
    for s,e in intervals[1:]:
        if s>=end: keep+=1; end=e
    return len(intervals)-keep
