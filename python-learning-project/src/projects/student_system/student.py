"""Student class"""
class Student:
    def __init__(self, student_id, name, email):
        self.student_id = student_id
        self.name = name
        self.email = email
        self.grades = {}
    
    def add_grade(self, subject, grade):
        if 0 <= grade <= 100:
            self.grades[subject] = grade
            return True
        raise ValueError("Grade must be 0-100")
    
    def get_average(self):
        if not self.grades:
            return 0
        return sum(self.grades.values()) / len(self.grades)
    
    def to_dict(self):
        return {
            'student_id': self.student_id,
            'name': self.name,
            'email': self.email,
            'grades': self.grades
        }
    
    @classmethod
    def from_dict(cls, data):
        student = cls(data['student_id'], data['name'], data['email'])
        student.grades = data.get('grades', {})
        return student
    
    def __str__(self):
        return f"Student({self.name}, Avg: {self.get_average():.2f})"
