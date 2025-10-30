# Example 5: Find Path to Target Node (Preorder DFS)
class Node:
    def __init__(self, val, left=None, right=None):
        self.val, self.left, self.right = val, left, right

def path_to_node(root, target, path=None):
    if not root: return None
    if path is None: path = []
    path.append(root.val)
    if root.val == target:
        return path.copy()
    left = path_to_node(root.left, target, path)
    right = path_to_node(root.right, target, path)
    path.pop()
    return left or right

root = Node(1, Node(2, Node(4), Node(5)), Node(3))
print("Path to 5:", path_to_node(root, 5))  # [1, 2, 5]
