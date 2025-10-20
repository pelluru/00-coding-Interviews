"""Exception handling"""
def divide(a, b):
    try:
        return a / b
    except ZeroDivisionError:
        print("Cannot divide by zero!")
        return None

print(divide(10, 2))
print(divide(10, 0))

# Custom exception
class CustomError(Exception):
    pass

try:
    raise CustomError("Something went wrong")
except CustomError as e:
    print(f"Caught: {e}")
