#!/usr/bin/env python3
"""Example 5: Applications of modpow / nCr / gcd-lcm
1) Number of length-k strings over alphabet size m: m^k % mod  (using modpow)
2) Number of non-empty subsets of an n-set: (2^n - 1) % mod
3) LCM over a list of numbers
"""
from typing import List

def modpow(a: int, e: int, mod: int) -> int:
    res = 1 % mod; a %= mod
    while e:
        if e & 1: res = (res * a) % mod
        a = (a * a) % mod; e >>= 1
    return res

def gcd(a: int, b: int) -> int:
    while b: a, b = b, a % b
    return abs(a)

def lcm(a: int, b: int) -> int:
    g = gcd(a,b)
    return 0 if a == 0 or b == 0 else abs(a // g * b)

def lcm_list(arr: List[int]) -> int:
    from functools import reduce
    return reduce(lcm, arr, 1)

if __name__ == "__main__":
    MOD = 1_000_000_007
    m, k = 26, 10
    print("Example 5 — m^k % MOD for m=26,k=10:", modpow(m, k, MOD))
    n = 5
    print("Example 5 — non-empty subsets of n=5 mod MOD:", (modpow(2, n, MOD) - 1) % MOD)
    arr = [4, 6, 8, 14]
    print("Example 5 — lcm over list [4,6,8,14]:", lcm_list(arr))
