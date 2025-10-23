"""
Dict Interview: Word pattern matches pattern <-> words bijection
Run: python word_pattern.py
"""
def word_pattern(pattern, s):
    words = s.split()
    if len(pattern) != len(words): return False
    m1, m2 = {}, {}
    for p, w in zip(pattern, words):
        if m1.get(p,w) != w or m2.get(w,p) != p:
            return False
        m1[p] = w; m2[w] = p
    return True

def main():
    print(word_pattern("abba", "dog cat cat dog"), word_pattern("abba","dog cat cat fish"))

if __name__ == "__main__":
    main()
