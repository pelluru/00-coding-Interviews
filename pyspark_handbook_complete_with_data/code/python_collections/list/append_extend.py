"""
List example 1: append vs extend
Run: python append_extend.py
"""
def main():
    nums = [1, 2, 3]
    nums.append([4, 5])
    after_append = nums[:]             # [1, 2, 3, [4,5]]
    nums = [1, 2, 3]
    nums.extend([4, 5])
    after_extend = nums[:]             # [1, 2, 3, 4, 5]
    print("after_append:", after_append)
    print("after_extend:", after_extend)

if __name__ == "__main__":
    main()
