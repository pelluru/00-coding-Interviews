"""
Dict Interview: Isomorphic strings (two-way mapping)
Run: python isomorphic_strings.py
"""
def is_isomorphic(s, t):
    if len(s) != len(t): return False
    m1, m2 = {}, {}
    for a,b in zip(s,t):
        if m1.get(a,b) != b or m2.get(b,a) != a:
            return False
        m1[a] = b; m2[b] = a
    return True

def main():
    print(is_isomorphic("egg","add"), is_isomorphic("foo","bar"))

if __name__ == "__main__":
    main()
