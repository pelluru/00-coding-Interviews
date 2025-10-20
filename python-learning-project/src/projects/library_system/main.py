"""Library System Main"""
from book import Book
from library import Library

def main():
    lib = Library("City Library")
    
    while True:
        print(f"\n=== {lib.name} ===")
        print("1. Add Book")
        print("2. Checkout Book")
        print("3. List Available")
        print("4. Exit")
        
        choice = input("Choice: ").strip()
        
        if choice == '1':
            title = input("Title: ")
            author = input("Author: ")
            isbn = input("ISBN: ")
            lib.add_book(Book(title, author, isbn))
            print("✓ Book added!")
        
        elif choice == '2':
            isbn = input("ISBN: ")
            print(lib.checkout_book(isbn))
        
        elif choice == '3':
            for book in lib.list_available():
                print(f"  {book}")
        
        elif choice == '4':
            break

if __name__ == "__main__":
    main()
