"""
Set Interview: Intersection of two arrays (unique results)
Run: python intersection_two_arrays.py
"""
def intersection(a, b):
    return sorted(set(a) & set(b))

def main():
    print(intersection([1,2,2,1], [2,2]))

if __name__ == "__main__":
    main()
