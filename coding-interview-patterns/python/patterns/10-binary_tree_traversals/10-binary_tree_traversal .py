"""
Binary_tree_traversal Algorithms — Practical Edition
====================================================
Covers:
1️⃣ Classic Traversals (DFS + BFS)
   • Preorder  (Root → Left → Right)
   • Inorder   (Left → Root → Right)
   • Postorder (Left → Right → Root)
   • Level Order (BFS)
2️⃣ Practical Real-World Problems Using Traversals
   • Example 1: Sum of all nodes
   • Example 2: Maximum depth of a tree
   • Example 3: Symmetric Tree check
   • Example 4: Path Sum existence
"""

from collections import deque
from typing import Optional, List


# -------------------------------------------------------------------
# Binary Tree Node definition
# -------------------------------------------------------------------
class TreeNode:
    def __init__(self, val: int, left: Optional['TreeNode'] = None, right: Optional['TreeNode'] = None):
        self.val = val
        self.left = left
        self.right = right


# -------------------------------------------------------------------
# 1️⃣ Preorder Traversal (Root → Left → Right)
# -------------------------------------------------------------------
def preorder_traversal(root: Optional[TreeNode]) -> List[int]:
    """Iterative preorder traversal using stack."""
    if not root:
        return []
    result, stack = [], [root]
    while stack:
        node = stack.pop()
        result.append(node.val)
        if node.right:
            stack.append(node.right)
        if node.left:
            stack.append(node.left)
    return result


# -------------------------------------------------------------------
# 2️⃣ Inorder Traversal (Left → Root → Right)
# -------------------------------------------------------------------
def inorder_traversal(root: Optional[TreeNode]) -> List[int]:
    """Iterative inorder traversal using stack."""
    result, stack = [], []
    cur = root
    while cur or stack:
        while cur:
            stack.append(cur)
            cur = cur.left
        cur = stack.pop()
        result.append(cur.val)
        cur = cur.right
    return result


# -------------------------------------------------------------------
# 3️⃣ Postorder Traversal (Left → Right → Root)
# -------------------------------------------------------------------
def postorder_traversal(root: Optional[TreeNode]) -> List[int]:
    """Iterative postorder traversal using reverse trick."""
    if not root:
        return []
    stack, result = [root], []
    while stack:
        node = stack.pop()
        result.append(node.val)
        if node.left:
            stack.append(node.left)
        if node.right:
            stack.append(node.right)
    return result[::-1]


# -------------------------------------------------------------------
# 4️⃣ Level Order Traversal (Breadth-First)
# -------------------------------------------------------------------
def level_order_traversal(root: Optional[TreeNode]) -> List[List[int]]:
    """BFS traversal by levels."""
    if not root:
        return []
    result = []
    queue = deque([root])
    while queue:
        level_size = len(queue)
        level = []
        for _ in range(level_size):
            node = queue.popleft()
            level.append(node.val)
            if node.left:
                queue.append(node.left)
            if node.right:
                queue.append(node.right)
        result.append(level)
    return result


# -------------------------------------------------------------------
# 🌟 Practical Example 1: Sum of all nodes
# -------------------------------------------------------------------
def sum_of_nodes(root: Optional[TreeNode]) -> int:
    """Use DFS (preorder) to compute sum of all nodes."""
    if not root:
        return 0
    return root.val + sum_of_nodes(root.left) + sum_of_nodes(root.right)
# Algorithm: Recursive DFS visiting every node once — O(n) time.


# -------------------------------------------------------------------
# 🌟 Practical Example 2: Maximum Depth of Binary Tree
# -------------------------------------------------------------------
def max_depth(root: Optional[TreeNode]) -> int:
    """Use recursion (DFS) to compute tree depth."""
    if not root:
        return 0
    left_depth = max_depth(root.left)
    right_depth = max_depth(root.right)
    return 1 + max(left_depth, right_depth)
# Algorithm: DFS returns 1 + max(left, right).  Time O(n), Space O(h) (height).


# -------------------------------------------------------------------
# 🌟 Practical Example 3: Check if Tree is Symmetric (Mirror)
# -------------------------------------------------------------------
def is_symmetric(root: Optional[TreeNode]) -> bool:
    """Check if tree is symmetric using BFS pairwise comparison."""
    if not root:
        return True
    queue = deque([(root.left, root.right)])
    while queue:
        l, r = queue.popleft()
        if not l and not r:
            continue
        if not l or not r or l.val != r.val:
            return False
        queue.append((l.left, r.right))
        queue.append((l.right, r.left))
    return True
# Algorithm: BFS checks mirrored pairs level by level. Time O(n).


# -------------------------------------------------------------------
# 🌟 Practical Example 4: Path Sum Existence
# -------------------------------------------------------------------
def has_path_sum(root: Optional[TreeNode], target_sum: int) -> bool:
    """Check if any root-to-leaf path sums to target."""
    if not root:
        return False
    # If leaf node, check if remaining sum equals node value
    if not root.left and not root.right:
        return root.val == target_sum
    new_sum = target_sum - root.val
    return has_path_sum(root.left, new_sum) or has_path_sum(root.right, new_sum)
# Algorithm: Recursive DFS exploring all root-to-leaf paths. Time O(n).


# -------------------------------------------------------------------
# 🧪 Main: Demonstration of Traversals + Practical Examples
# -------------------------------------------------------------------
def main():
    """
    Build sample tree:
             1
           /   \
          2     3
         / \   / \
        4  5  6  7
    """
    root = TreeNode(1,
                    TreeNode(2, TreeNode(4), TreeNode(5)),
                    TreeNode(3, TreeNode(6), TreeNode(7)))

    print("\n=== TREE TRAVERSALS ===")
    print("Preorder (Root→Left→Right):", preorder_traversal(root))
    print("Inorder (Left→Root→Right):", inorder_traversal(root))
    print("Postorder (Left→Right→Root):", postorder_traversal(root))
    print("Level Order (BFS):", level_order_traversal(root))

    print("\n=== PRACTICAL EXAMPLES ===")
    print("1️⃣ Sum of all nodes:", sum_of_nodes(root))                # 1+2+3+4+5+6+7 = 28
    print("2️⃣ Maximum depth of tree:", max_depth(root))              # 3
    print("3️⃣ Is tree symmetric?:", is_symmetric(root))              # False
    print("4️⃣ Has path sum = 7?:", has_path_sum(root, 7))           # True (path 1→2→4)


if __name__ == "__main__":
    main()
