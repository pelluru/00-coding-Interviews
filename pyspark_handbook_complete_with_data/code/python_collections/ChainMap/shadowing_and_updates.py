"""
ChainMap example 2: shadowing and new_child
Run: python shadowing_and_updates.py
"""
from collections import ChainMap

def main():
    base = {"a":1}
    overlay = {"a":2}
    cm = ChainMap(overlay, base)
    print(cm["a"])           # 2
    child = cm.new_child({"b":3})
    print(child["b"], child["a"])

if __name__ == "__main__":
    main()
