"""
Linked List — In-place Reversal
===============================

This module demonstrates how to reverse a singly linked list *in-place*,
i.e., without using extra memory for another list.

Algorithm: Iterative In-place Reversal
--------------------------------------
We maintain three pointers:
    prev → previous node (initially None)
    curr → current node we're processing
    nxt  → temporarily stores the next node (to not lose the link)

At each step:
    1. Save next node (nxt = curr.next)
    2. Reverse the link (curr.next = prev)
    3. Move prev forward (prev = curr)
    4. Move curr forward (curr = nxt)
When curr becomes None, prev is the new head of the reversed list.

This runs in linear time and constant space.

Complexity:
------------
Time  O(n)  — each node is visited once
Space O(1)  — reversal done in place
"""


# ---------------------------------------------------------------------------
# Linked List Node Definition
# ---------------------------------------------------------------------------

class ListNode:
    def __init__(self, val, nxt=None):
        self.val = val
        self.next = nxt

    def __repr__(self):
        return f"ListNode({self.val})"


# ---------------------------------------------------------------------------
# In-place Reversal Algorithm
# ---------------------------------------------------------------------------

def reverse_linked_list(head):
    """
    Reverse a singly linked list in place and return the new head.

    Example:
        Input : 1 -> 2 -> 3 -> 4 -> 5 -> None
        Output: 5 -> 4 -> 3 -> 2 -> 1 -> None
    """
    prev = None
    curr = head

    while curr:
        nxt = curr.next      # 1️⃣ Store next node
        curr.next = prev     # 2️⃣ Reverse the link
        prev = curr          # 3️⃣ Move prev one step forward
        curr = nxt           # 4️⃣ Move curr one step forward

    # At the end, prev points to the new head
    return prev


# ---------------------------------------------------------------------------
# Helper Utilities
# ---------------------------------------------------------------------------

def build_linked_list(values):
    """Create a linked list from a Python list and return its head."""
    if not values:
        return None
    head = ListNode(values[0])
    curr = head
    for v in values[1:]:
        curr.next = ListNode(v)
        curr = curr.next
    return head


def linked_list_to_list(head):
    """Convert a linked list back to a regular Python list (for printing)."""
    result = []
    while head:
        result.append(head.val)
        head = head.next
    return result


# ---------------------------------------------------------------------------
# Main Demonstration
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== Linked List In-place Reversal Demo ===\n")

    # Build the linked list 1 -> 2 -> 3 -> 4 -> 5
    values = [1, 2, 3, 4, 5]
    head = build_linked_list(values)
    print("Original list:", linked_list_to_list(head))

    # Reverse the linked list
    new_head = reverse_linked_list(head)
    print("Reversed list:", linked_list_to_list(new_head))

    # Try another example
    values2 = [10, 20, 30]
    head2 = build_linked_list(values2)
    print("\nOriginal list:", linked_list_to_list(head2))
    print("Reversed list:", linked_list_to_list(reverse_linked_list(head2)))

    # Edge cases
    print("\nEdge case (empty list):", linked_list_to_list(reverse_linked_list(None)))
    one_node = build_linked_list([42])
    print("Edge case (single node):", linked_list_to_list(reverse_linked_list(one_node)))
