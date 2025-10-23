def modpow(a,e,mod):
    res=1%mod; a%=mod
    while e:
        if e&1: res=(res*a)%mod
        a=(a*a)%mod; e>>=1
    return res

def nCr_mod(n,r,mod):
    if r<0 or r>n: return 0
    fact=[1]*(n+1); inv=[1]*(n+1)
    for i in range(1,n+1): fact[i]=(fact[i-1]*i)%mod
    inv[n]=pow(fact[n], mod-2, mod)
    for i in range(n,0,-1): inv[i-1]=(inv[i]*i)%mod
    return (((fact[n]*inv[r])%mod)*inv[n-r])%mod

def gcd(a,b):
    while b: a,b=b,a%b
    return a
def lcm(a,b):
    from math import prod
    return a//gcd(a,b)*b
