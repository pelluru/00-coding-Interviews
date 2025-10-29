#!/usr/bin/env python3
"""Example 4: nPr % mod using factorials and modular inverse
nPr = n! / (n - r)!  ->  fact[n] * inv_fact[n-r]  (mod must be prime to use Fermat inverse)

We reuse the same precompute pattern as nCr_mod.
"""
def nPr_mod(n: int, r: int, mod: int) -> int:
    if r < 0 or r > n:
        return 0
    fact = [1] * (n + 1)
    inv = [1] * (n + 1)
    for i in range(1, n + 1):
        fact[i] = (fact[i - 1] * i) % mod
    inv[n] = pow(fact[n], mod - 2, mod)  # Fermat inverse, prime mod
    for i in range(n, 0, -1):
        inv[i - 1] = (inv[i] * i) % mod
    return (fact[n] * inv[n - r]) % mod

if __name__ == "__main__":
    MOD = 1_000_000_007
    print("Example 4 — nPr_mod(10, 3, MOD) =", nPr_mod(10, 3, MOD))  # 720
