"""
Set Interview: Happy number (detect cycle via set)
Run: python happy_number.py
"""
def is_happy(n):
    seen = set()
    def nxt(x): 
        s = 0
        while x:
            s += (x%10)**2
            x //= 10
        return s
    while n != 1 and n not in seen:
        seen.add(n)
        n = nxt(n)
    return n == 1

def main():
    print(is_happy(19), is_happy(2))

if __name__ == "__main__":
    main()
