# Example 2: Serialize and Deserialize Tree (Preorder Traversal)
class Node:
    def __init__(self, val, left=None, right=None):
        self.val, self.left, self.right = val, left, right

def serialize(root):
    if not root: return "None,"
    return str(root.val) + "," + serialize(root.left) + serialize(root.right)

def deserialize(data):
    vals = iter(data.split(','))
    def build():
        val = next(vals)
        if val == "None": return None
        node = Node(int(val))
        node.left = build()
        node.right = build()
        return node
    return build()

root = Node(1, Node(2, Node(4), Node(5)), Node(3))
s = serialize(root)
print("Serialized:", s)
new_root = deserialize(s)
print("Deserialized Root Value:", new_root.val)
