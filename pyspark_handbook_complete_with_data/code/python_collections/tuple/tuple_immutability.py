"""
Tuple example 3: immutability and nested objects
Run: python tuple_immutability.py
"""
def main():
    t = ([], 1)
    try:
        # t[0] = 5  # would error
        t[0].append("mutable_inside")
        print(t)
    except TypeError as e:
        print("TypeError:", e)

if __name__ == "__main__":
    main()
