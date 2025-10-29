#!/usr/bin/env python3
"""Math Counting — Five Examples in One File
Includes:
  1) modpow (binary exponentiation)
  2) nCr_mod (Fermat inverse)
  3) gcd / lcm
  4) nPr_mod
  5) Applications (m^k, subset count, lcm over list)

Run:
  python math_counting_all_examples.py
  python math_counting_all_examples.py 1 3 5
"""
def modpow(a: int, e: int, mod: int) -> int:
    res = 1 % mod; a %= mod
    while e:
        if e & 1: res = (res * a) % mod
        a = (a * a) % mod; e >>= 1
    return res

def nCr_mod(n: int, r: int, mod: int) -> int:
    if r < 0 or r > n: return 0
    fact = [1] * (n + 1); inv = [1] * (n + 1)
    for i in range(1, n + 1): fact[i] = (fact[i - 1] * i) % mod
    inv[n] = pow(fact[n], mod - 2, mod)
    for i in range(n, 0, -1): inv[i - 1] = (inv[i] * i) % mod
    return (((fact[n] * inv[r]) % mod) * inv[n - r]) % mod

def gcd(a: int, b: int) -> int:
    while b: a, b = b, a % b
    return abs(a)

def lcm(a: int, b: int) -> int:
    g = gcd(a, b)
    return 0 if a == 0 or b == 0 else abs(a // g * b)

def nPr_mod(n: int, r: int, mod: int) -> int:
    if r < 0 or r > n: return 0
    fact = [1] * (n + 1); inv = [1] * (n + 1)
    for i in range(1, n + 1): fact[i] = (fact[i - 1] * i) % mod
    inv[n] = pow(fact[n], mod - 2, mod)
    for i in range(n, 0, -1): inv[i - 1] = (inv[i] * i) % mod
    return (fact[n] * inv[n - r]) % mod

def run_example_1():
    print("Example 1 — modpow(3, 13, 1e9+7):", modpow(3, 13, 1_000_000_007))

def run_example_2():
    MOD = 1_000_000_007
    print("Example 2 — nCr_mod(10, 3, MOD):", nCr_mod(10, 3, MOD))

def run_example_3():
    print("Example 3 — gcd(48, 18):", gcd(48, 18))
    print("Example 3 — lcm(21, 6):", lcm(21, 6))

def run_example_4():
    MOD = 1_000_000_007
    print("Example 4 — nPr_mod(10, 3, MOD):", nPr_mod(10, 3, MOD))

def run_example_5():
    MOD = 1_000_000_007
    print("Example 5 — 26^10 % MOD:", modpow(26, 10, MOD))
    print("Example 5 — (2^5 - 1) % MOD:", (modpow(2, 5, MOD) - 1) % MOD)
    # lcm over a list
    def lcm_list(arr):
        from functools import reduce
        def _lcm(a, b):
            g = gcd(a, b)
            return 0 if a == 0 or b == 0 else abs(a // g * b)
        return reduce(_lcm, arr, 1)
    print("Example 5 — lcm([4,6,8,14]):", lcm_list([4,6,8,14]))

def _run_all(selected=None):
    mapping = {1: run_example_1, 2: run_example_2, 3: run_example_3, 4: run_example_4, 5: run_example_5}
    order = selected or [1,2,3,4,5]
    for i in order:
        mapping[i]()

if __name__ == "__main__":
    import sys
    if len(sys.argv) == 1: _run_all()
    else: _run_all([int(x) for x in sys.argv[1:]])
