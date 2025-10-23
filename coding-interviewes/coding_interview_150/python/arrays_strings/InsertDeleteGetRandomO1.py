
"""Insert Delete GetRandom O(1) — array+map. Avg O(1) per op"""
import random
class RandomizedSet:
    def __init__(self): self.arr=[]; self.pos={}
    def insert(self,v):
        if v in self.pos: return False
        self.pos[v]=len(self.arr); self.arr.append(v); return True
    def remove(self,v):
        if v not in self.pos: return False
        i=self.pos[v]; last=self.arr[-1]
        self.arr[i]=last; self.pos[last]=i
        self.arr.pop(); del self.pos[v]; return True
    def getRandom(self): return random.choice(self.arr)
def main():
    rs=RandomizedSet(); print(rs.insert(1)); print(rs.remove(2)); print(rs.insert(2)); print(rs.getRandom() in {1,2})
if __name__=='__main__': main()
