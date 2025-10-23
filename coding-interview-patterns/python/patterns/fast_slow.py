class ListNode:
    def __init__(self,x,nxt=None): self.val=x; self.next=nxt
def has_cycle(head):
    slow=fast=head
    while fast and fast.next:
        slow=slow.next; fast=fast.next.next
        if slow is fast: return True
    return False

def detect_cycle_entry(head):
    slow=fast=head
    while fast and fast.next:
        slow=slow.next; fast=fast.next.next
        if slow is fast:
            slow=head
            while slow is not fast:
                slow=slow.next; fast=fast.next
            return slow
    return None

def middle_node(head):
    slow=fast=head
    while fast and fast.next:
        slow=slow.next; fast=fast.next.next
    return slow
