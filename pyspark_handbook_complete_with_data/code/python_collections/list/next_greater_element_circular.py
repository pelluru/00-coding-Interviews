"""
List Interview: Next greater element (circular array) using stack (list)
Run: python next_greater_element_circular.py
"""
def next_greater_elements(nums):
    n = len(nums)
    ans = [-1]*n
    st = []  # stack of indices
    for i in range(2*n):
        x = nums[i % n]
        while st and nums[st[-1]] < x:
            ans[st.pop()] = x
        if i < n:
            st.append(i)
    return ans

def main():
    print(next_greater_elements([1,2,1]))

if __name__ == "__main__":
    main()
