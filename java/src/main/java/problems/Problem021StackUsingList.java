package problems;


import java.util.*;
public class Problem021StackUsingList {
    public static class StackX<T>{LinkedList<T> s=new LinkedList<>(); public void push(T x){s.addLast(x);} public T pop(){return s.removeLast();} public T peek(){return s.peekLast();} public boolean isEmpty(){return s.isEmpty();}}
}

