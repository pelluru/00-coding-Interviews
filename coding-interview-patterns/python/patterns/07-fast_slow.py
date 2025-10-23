"""
Fast and Slow Pointer Algorithms on Linked Lists
================================================

These algorithms use two pointers that move at different speeds
(usually one moves one step at a time, and the other moves two steps).

This technique is called Floyd’s Tortoise and Hare approach.
It’s widely used for detecting cycles and finding middle nodes efficiently.

Algorithms included:
1. has_cycle(head)          — detect whether a cycle exists
2. detect_cycle_entry(head) — find the node where a cycle begins
3. middle_node(head)        — find the middle node of a singly linked list
"""

# ---------------------------------------------------------------------------
# Linked List Node Definition
# ---------------------------------------------------------------------------

class ListNode:
    def __init__(self, x, nxt=None):
        self.val = x
        self.next = nxt

    def __repr__(self):
        """Readable representation for debugging."""
        return f"ListNode({self.val})"


# ---------------------------------------------------------------------------
# 1. Detect if a linked list has a cycle
# ---------------------------------------------------------------------------

def has_cycle(head: ListNode) -> bool:
    """
    Determine if a linked list has a cycle (loop).

    Algorithm (Floyd’s Tortoise and Hare):
    --------------------------------------
    - Use two pointers: `slow` moves 1 step, `fast` moves 2 steps.
    - If there is no cycle, `fast` or `fast.next` will become None (end of list).
    - If there is a cycle, `fast` will eventually "lap" and meet `slow`.

    Returns:
        True  — if a cycle exists
        False — otherwise

    Complexity:
        Time  O(n)
        Space O(1)
    """
    slow = fast = head
    while fast and fast.next:
        slow = slow.next
        fast = fast.next.next
        if slow is fast:  # pointers meet → cycle exists
            return True
    return False


# ---------------------------------------------------------------------------
# 2. Find the starting node of the cycle (if it exists)
# ---------------------------------------------------------------------------

def detect_cycle_entry(head: ListNode):
    """
    Return the node where the cycle begins, or None if no cycle.

    Algorithm:
    ----------
    - First, use has_cycle() logic to detect intersection point of fast & slow.
    - Once they meet inside the cycle:
        * Move one pointer back to head.
        * Move both pointers one step at a time.
        * They will meet again at the cycle entry node.
    - Why it works:
        Let the distance from head to cycle start = a,
            distance from cycle start to meeting point = b,
            cycle length = c.
        Then 2(a + b) = a + b + k*c  ⇒ a = k*c - b,
        so moving one pointer to head and both at 1x speed makes them meet
        exactly at cycle start.

    Complexity:
        Time  O(n)
        Space O(1)
    """
    slow = fast = head
    while fast and fast.next:
        slow = slow.next
        fast = fast.next.next
        if slow is fast:  # found intersection
            slow = head
            while slow is not fast:
                slow = slow.next
                fast = fast.next
            return slow  # start of cycle
    return None


# ---------------------------------------------------------------------------
# 3. Find the middle node of a linked list
# ---------------------------------------------------------------------------

def middle_node(head: ListNode):
    """
    Return the middle node of a singly linked list.

    Algorithm:
    ----------
    - Move `fast` two steps and `slow` one step at a time.
    - When `fast` reaches the end, `slow` will be at the middle.

    Notes:
        If there are two middle nodes (even-length list),
        the function returns the **second** middle node.

    Complexity:
        Time  O(n)
        Space O(1)
    """
    slow = fast = head
    while fast and fast.next:
        slow = slow.next
        fast = fast.next.next
    return slow


# ---------------------------------------------------------------------------
# Utility: Build linked list from list
# ---------------------------------------------------------------------------

def build_linked_list(values):
    """Helper to create a singly linked list from a Python list."""
    if not values:
        return None
    head = ListNode(values[0])
    cur = head
    for v in values[1:]:
        cur.next = ListNode(v)
        cur = cur.next
    return head


# ---------------------------------------------------------------------------
# Main Demonstration
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== Fast–Slow Pointer Algorithms Demo ===\n")

    # 1️⃣ Non-cyclic list
    head1 = build_linked_list([1, 2, 3, 4, 5])
    print("List 1:", [1, 2, 3, 4, 5])
    print("Has cycle:", has_cycle(head1))
    print("Middle node:", middle_node(head1).val)
    print("Cycle entry:", detect_cycle_entry(head1), "\n")

    # 2️⃣ Create a list with a cycle: 1 -> 2 -> 3 -> 4 -> 5 -> (back to 3)
    nodes = [ListNode(i) for i in range(1, 6)]
    for i in range(4):
        nodes[i].next = nodes[i + 1]
    nodes[-1].next = nodes[2]  # tail connects to node with value 3 (cycle)

    head2 = nodes[0]
    print("List 2 (with cycle): 1 -> 2 -> 3 -> 4 -> 5 -> (back to 3)")
    print("Has cycle:", has_cycle(head2))
    entry = detect_cycle_entry(head2)
    print("Cycle entry node value:", entry.val if entry else None)
    print("Middle node (nonsensical for cycles):", middle_node(head2).val)
