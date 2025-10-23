"""
List example 2: list comprehension with condition
Run: python list_comprehension.py
"""
def main():
    squares_of_evens = [x*x for x in range(10) if x % 2 == 0]
    print(squares_of_evens)

if __name__ == "__main__":
    main()
