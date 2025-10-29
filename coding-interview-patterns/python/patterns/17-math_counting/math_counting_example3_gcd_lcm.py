#!/usr/bin/env python3
"""Example 3: gcd and lcm
- gcd via Euclidean algorithm in O(log min(a,b))
- lcm(a,b) = a // gcd(a,b) * b  (use // before * to avoid overflow)
"""
def gcd(a: int, b: int) -> int:
    while b:
        a, b = b, a % b
    return abs(a)

def lcm(a: int, b: int) -> int:
    g = gcd(a, b)
    return 0 if a == 0 or b == 0 else abs(a // g * b)

if __name__ == "__main__":
    print("Example 3 — gcd(48, 18) =", gcd(48, 18))   # 6
    print("Example 3 — lcm(21, 6)  =", lcm(21, 6))    # 42
