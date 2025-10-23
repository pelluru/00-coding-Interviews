"""
Tuple Interview: Minimum meeting rooms using start/end tuples
Run: python min_meeting_rooms.py
"""
def min_meeting_rooms(intervals):
    starts = sorted(s for s,_ in intervals)
    ends   = sorted(e for _,e in intervals)
    i = j = rooms = ans = 0
    while i < len(starts):
        if starts[i] < ends[j]:
            rooms += 1; ans = max(ans, rooms); i += 1
        else:
            rooms -= 1; j += 1
    return ans

def main():
    print(min_meeting_rooms([(0,30),(5,10),(15,20)]))

if __name__ == "__main__":
    main()
