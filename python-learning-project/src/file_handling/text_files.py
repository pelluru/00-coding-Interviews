"""Text file operations"""
def write_text(filename, content):
    with open(filename, 'w') as f:
        f.write(content)

def read_text(filename):
    try:
        with open(filename, 'r') as f:
            return f.read()
    except FileNotFoundError:
        return "File not found"

if __name__ == "__main__":
    write_text('test.txt', 'Hello World!')
    print(read_text('test.txt'))
