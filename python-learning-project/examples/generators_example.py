"""Generator examples"""
def countdown(n):
    while n > 0:
        yield n
        n -= 1

for i in countdown(5):
    print(i)

# Generator expression
squares = (x**2 for x in range(10))
print(next(squares))
print(next(squares))
