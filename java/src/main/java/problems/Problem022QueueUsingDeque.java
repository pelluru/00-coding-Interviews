package problems;


import java.util.*;
public class Problem022QueueUsingDeque {
    public static class QueueX<T>{ArrayDeque<T> q=new ArrayDeque<>(); public void enqueue(T x){q.addLast(x);} public T dequeue(){return q.pollFirst();} public boolean isEmpty(){return q.isEmpty();} }
}

