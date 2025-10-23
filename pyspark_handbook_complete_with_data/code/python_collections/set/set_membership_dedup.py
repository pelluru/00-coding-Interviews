"""
Set example 1: membership + dedup
Run: python set_membership_dedup.py
"""
def main():
    nums = [1,2,2,3]
    unique = set(nums)
    print("unique:", unique, "has 2?", 2 in unique)

if __name__ == "__main__":
    main()
