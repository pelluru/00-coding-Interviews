# Example 4: Compute Height of a Binary Tree (Postorder Traversal)
class Node:
    def __init__(self, val, left=None, right=None):
        self.val, self.left, self.right = val, left, right

def height(root):
    if not root:
        return 0
    return 1 + max(height(root.left), height(root.right))

root = Node(1, Node(2, Node(4)), Node(3))
print("Tree Height:", height(root))  # 3
