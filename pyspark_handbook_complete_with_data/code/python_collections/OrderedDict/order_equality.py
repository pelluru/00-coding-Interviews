"""
OrderedDict example 3: order-aware equality (pre-3.7 dict difference)
Run: python order_equality.py
"""
from collections import OrderedDict

def main():
    od1 = OrderedDict([("a",1),("b",2)])
    od2 = OrderedDict([("b",2),("a",1)])
    print("od1 == od2?", od1 == od2)

if __name__ == "__main__":
    main()
