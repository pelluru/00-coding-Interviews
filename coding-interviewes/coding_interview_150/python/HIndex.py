
"""H-Index — sort and scan. Time: O(n log n)"""
def h_index(citations):
    citations.sort(reverse=True)
    h=0
    for i,c in enumerate(citations,1):
        if c>=i: h=i
    return h
def main():
    print(h_index([3,0,6,1,5]))
if __name__=='__main__': main()
