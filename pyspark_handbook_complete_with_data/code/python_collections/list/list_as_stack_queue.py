"""
List example 3: using list as stack/queue (small scale)
Run: python list_as_stack_queue.py
"""
def main():
    st = []
    st.append(1); st.append(2); st.append(3)
    print("stack_pop:", st.pop())  # 3

    q = [1,2,3]
    front = q.pop(0)               # O(n), consider deque for perf
    print("queue_pop_front:", front, "remaining:", q)

if __name__ == "__main__":
    main()
