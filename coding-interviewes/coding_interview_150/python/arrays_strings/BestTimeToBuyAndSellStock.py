
"""Best Time to Buy and Sell Stock (single transaction)
Algorithm:
1) Track min price so far and best profit.
Time: O(n), Space: O(1)"""
def max_profit(prices):
    mn=float('inf'); ans=0
    for p in prices:
        if p<mn: mn=p
        if p-mn>ans: ans=p-mn
    return ans
def main():
    print(max_profit([7,1,5,3,6,4]))
    print(max_profit([7,6,4,3,1]))
if __name__=='__main__': main()
