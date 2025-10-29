#!/usr/bin/env python3
"""Example 2: nCr % mod using factorials and modular inverses (Fermat's Little Theorem)
Assumes mod is prime (so that x^(mod-2) is the modular inverse of x).

Precompute:
  fact[i] = i! % mod
  inv[i]  = (i!)^{-1} % mod  using inv[n] = fact[n]^(mod-2) and inv[i-1] = inv[i] * i % mod

nCr(n, r) % mod = fact[n] * inv[r] % mod * inv[n-r] % mod
Complexity: O(n) precompute, O(1) per query after precompute.
"""
def nCr_mod(n: int, r: int, mod: int) -> int:
    if r < 0 or r > n:
        return 0
    fact = [1] * (n + 1)
    inv = [1] * (n + 1)
    for i in range(1, n + 1):
        fact[i] = (fact[i - 1] * i) % mod
    # Fermat inverse: inv[n] = fact[n]^(mod-2) % mod (mod must be prime)
    inv[n] = pow(fact[n], mod - 2, mod)
    for i in range(n, 0, -1):
        inv[i - 1] = (inv[i] * i) % mod
    return (((fact[n] * inv[r]) % mod) * inv[n - r]) % mod

if __name__ == "__main__":
    MOD = 1_000_000_007
    print("Example 2 — nCr_mod(10, 3, MOD) =", nCr_mod(10, 3, MOD))  # 120
