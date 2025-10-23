def next_greater(nums):
    st=[]; res=[-1]*len(nums)
    for i,x in enumerate(nums):
        while st and nums[st[-1]]<x:
            res[st.pop()]=x
        st.append(i)
    return res

def daily_temperatures(T):
    st=[]; res=[0]*len(T)
    for i,t in enumerate(T):
        while st and T[st[-1]]<t:
            j=st.pop(); res[j]=i-j
        st.append(i)
    return res

def largest_rectangle(h):
    st=[]; ans=0; h.append(0)
    for i,x in enumerate(h):
        while st and h[st[-1]]>x:
            H=h[st.pop()]; L=st[-1] if st else -1
            ans=max(ans, H*(i-L-1))
        st.append(i)
    h.pop(); return ans
