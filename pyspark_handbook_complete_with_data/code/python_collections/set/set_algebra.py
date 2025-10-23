"""
Set example 2: algebra (union, intersection, diff, xor)
Run: python set_algebra.py
"""
def main():
    A, B = {1,2,3}, {3,4}
    print("A&B:", A&B, "A|B:", A|B, "A-B:", A-B, "A^B:", A^B)

if __name__ == "__main__":
    main()
