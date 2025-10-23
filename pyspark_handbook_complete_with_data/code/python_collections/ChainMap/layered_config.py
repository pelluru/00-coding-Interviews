"""
ChainMap example 1: layered config lookup
Run: python layered_config.py
"""
from collections import ChainMap

def main():
    defaults = {"host":"localhost","port":5432}
    env = {"port":6543}
    cm = ChainMap(env, defaults)
    print(cm["port"], cm["host"])

if __name__ == "__main__":
    main()
