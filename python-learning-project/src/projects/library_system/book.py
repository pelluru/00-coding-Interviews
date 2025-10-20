"""Book class"""
class Book:
    def __init__(self, title, author, isbn):
        self.title = title
        self.author = author
        self.isbn = isbn
        self.is_checked_out = False
    
    def checkout(self):
        if not self.is_checked_out:
            self.is_checked_out = True
            return True
        return False
    
    def return_book(self):
        if self.is_checked_out:
            self.is_checked_out = False
            return True
        return False
    
    def __str__(self):
        status = "Checked Out" if self.is_checked_out else "Available"
        return f"{self.title} by {self.author} - {status}"
