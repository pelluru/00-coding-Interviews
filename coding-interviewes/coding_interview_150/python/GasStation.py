
"""Gas Station — greedy total+tank reset. Time: O(n)"""
def can_complete_circuit(gas,cost):
    total=tank=start=0
    for i in range(len(gas)):
        diff=gas[i]-cost[i]
        total+=diff; tank+=diff
        if tank<0: start=i+1; tank=0
    return start if total>=0 else -1
def main():
    print(can_complete_circuit([1,2,3,4,5],[3,4,5,1,2]))
if __name__=='__main__': main()
