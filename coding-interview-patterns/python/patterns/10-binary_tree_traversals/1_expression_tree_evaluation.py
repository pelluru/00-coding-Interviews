# Example 1: Expression Tree Evaluation (Postorder Traversal)
class Node:
    def __init__(self, val, left=None, right=None):
        self.val, self.left, self.right = val, left, right

def evaluate(root):
    if not root.left and not root.right:
        return int(root.val)
    left_val = evaluate(root.left)
    right_val = evaluate(root.right)
    if root.val == '+': return left_val + right_val
    if root.val == '-': return left_val - right_val
    if root.val == '*': return left_val * right_val
    if root.val == '/': return left_val / right_val

root = Node('+', Node('3'), Node('*', Node('2'), Node('5')))
print("Expression Value:", evaluate(root))  # 13
