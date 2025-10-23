def kmp_prefix(p):
    pi=[0]*len(p); j=0
    for i in range(1,len(p)):
        while j and p[i]!=p[j]: j=pi[j-1]
        if p[i]==p[j]: j+=1
        pi[i]=j
    return pi
def kmp_search(text, pat):
    if not pat: return 0
    pi=kmp_prefix(pat); j=0
    for i,ch in enumerate(text):
        while j and ch!=pat[j]: j=pi[j-1]
        if ch==pat[j]: j+=1
        if j==len(pat): return i-j+1
    return -1

def rabin_karp(text, pat, base=257, mod=10**9+7):
    n,m=len(text),len(pat)
    if m==0: return 0
    if m>n: return -1
    power=1
    for _ in range(m-1): power=(power*base)%mod
    th=ph=0
    for i in range(m):
        th=(th*base+ord(text[i]))%mod
        ph=(ph*base+ord(pat[i]))%mod
    for i in range(n-m+1):
        if th==ph and text[i:i+m]==pat: return i
        if i+m<n:
            th=((th-ord(text[i])*power)%mod+mod)%mod
            th=(th*base+ord(text[i+m]))%mod
    return -1

def is_rotation(a,b):
    if len(a)!=len(b): return False
    return kmp_search(a+a, b) != -1
