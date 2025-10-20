"""JSON operations"""
import json

def write_json(filename, data):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)

def read_json(filename):
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

if __name__ == "__main__":
    data = {'name': 'Alice', 'age': 30}
    write_json('person.json', data)
    print(read_json('person.json'))
