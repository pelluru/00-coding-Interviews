# Example 3: Verify if a Binary Tree is a BST (Inorder Traversal)
class Node:
    def __init__(self, val, left=None, right=None):
        self.val, self.left, self.right = val, left, right

def is_valid_bst(root):
    vals = []
    def inorder(node):
        if not node: return
        inorder(node.left)
        vals.append(node.val)
        inorder(node.right)
    inorder(root)
    return all(vals[i] < vals[i+1] for i in range(len(vals)-1))

root = Node(2, Node(1), Node(3))
print("Valid BST?", is_valid_bst(root))  # True
