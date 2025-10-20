"""Library class"""
class Library:
    def __init__(self, name):
        self.name = name
        self.books = []
    
    def add_book(self, book):
        self.books.append(book)
    
    def checkout_book(self, isbn):
        for book in self.books:
            if book.isbn == isbn:
                if book.checkout():
                    return f"Checked out: {book.title}"
                return "Already checked out"
        return "Book not found"
    
    def list_available(self):
        return [b for b in self.books if not b.is_checked_out]
