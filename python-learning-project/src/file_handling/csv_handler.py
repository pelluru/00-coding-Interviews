"""CSV operations"""
import csv

def write_csv(filename, data):
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(data)

def read_csv(filename):
    try:
        with open(filename, 'r') as f:
            return list(csv.reader(f))
    except FileNotFoundError:
        return []

if __name__ == "__main__":
    data = [['Name', 'Age'], ['Alice', '30'], ['Bob', '25']]
    write_csv('people.csv', data)
    print(read_csv('people.csv'))
