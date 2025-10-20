"""Student Management System Main"""
from student import Student
from database import StudentDatabase

def main():
    db = StudentDatabase()
    
    while True:
        print("\n=== Student System ===")
        print("1. Add Student")
        print("2. View Student")
        print("3. Add Grade")
        print("4. List Students")
        print("5. Exit")
        
        choice = input("Choice: ").strip()
        
        try:
            if choice == '1':
                sid = input("ID: ")
                name = input("Name: ")
                email = input("Email: ")
                db.add_student(Student(sid, name, email))
                print("✓ Added!")
            
            elif choice == '2':
                sid = input("ID: ")
                s = db.get_student(sid)
                if s:
                    print(s)
                    print(f"Grades: {s.grades}")
                else:
                    print("Not found")
            
            elif choice == '3':
                sid = input("ID: ")
                s = db.get_student(sid)
                if s:
                    subj = input("Subject: ")
                    grade = float(input("Grade: "))
                    s.add_grade(subj, grade)
                    db.save()
                    print("✓ Grade added!")
                else:
                    print("Not found")
            
            elif choice == '4':
                for s in db.list_all_students():
                    print(s)
            
            elif choice == '5':
                break
        
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()
