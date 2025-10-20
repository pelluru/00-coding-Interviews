"""Student database"""
import json
import os
from student import Student

class StudentDatabase:
    def __init__(self, filename='students.json'):
        self.filename = filename
        self.students = {}
        self.load()
    
    def load(self):
        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'r') as f:
                    data = json.load(f)
                    self.students = {
                        sid: Student.from_dict(sdata) 
                        for sid, sdata in data.items()
                    }
            except Exception as e:
                print(f"Error loading: {e}")
    
    def save(self):
        try:
            data = {sid: s.to_dict() for sid, s in self.students.items()}
            with open(self.filename, 'w') as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            print(f"Error saving: {e}")
    
    def add_student(self, student):
        if student.student_id in self.students:
            raise ValueError("Student ID exists")
        self.students[student.student_id] = student
        self.save()
    
    def get_student(self, student_id):
        return self.students.get(student_id)
    
    def list_all_students(self):
        return list(self.students.values())
