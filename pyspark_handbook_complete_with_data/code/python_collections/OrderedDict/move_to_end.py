"""
OrderedDict example 2: move_to_end
Run: python move_to_end.py
"""
from collections import OrderedDict

def main():
    od = OrderedDict([("a",1),("b",2),("c",3)])
    od.move_to_end("b")
    print(list(od.items()))

if __name__ == "__main__":
    main()
