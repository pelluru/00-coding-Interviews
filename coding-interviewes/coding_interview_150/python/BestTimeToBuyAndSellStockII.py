
"""Best Time II (unlimited transactions)
Sum positive day-to-day increases.
Time: O(n), Space: O(1)"""
def max_profit2(prices):
    ans=0
    for i in range(1,len(prices)):
        if prices[i]>prices[i-1]: ans+=prices[i]-prices[i-1]
    return ans
def main():
    print(max_profit2([7,1,5,3,6,4]))
    print(max_profit2([1,2,3,4,5]))
if __name__=='__main__': main()
