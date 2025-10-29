#!/usr/bin/env python3
"""Example 1: Fast Modular Exponentiation (Binary Exponentiation)
Compute (a^e) % mod in O(log e) time by repeated squaring.

Algorithm (right-to-left binary exponentiation):
  res = 1
  while e > 0:
    - if lowest bit of e is 1, multiply res by a
    - square a, shift e right by 1
Complexity: O(log e) multiplications.
"""
def modpow(a: int, e: int, mod: int) -> int:
    res = 1 % mod
    a %= mod
    while e:
        if e & 1:
            res = (res * a) % mod
        a = (a * a) % mod
        e >>= 1
    return res

if __name__ == "__main__":
    print("Example 1 — modpow(3, 13, 1000000007) =", modpow(3, 13, 1000000007))  # 1594323
