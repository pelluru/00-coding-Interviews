"""
namedtuple example 2: rename and defaults
Run: python rename_fields.py
"""
from collections import namedtuple

def main():
    User = namedtuple("User", ["id", "name", "role"])
    u = User(1, "Ada", "admin")
    print(u._asdict())

if __name__ == "__main__":
    main()
