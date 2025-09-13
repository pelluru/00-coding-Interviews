package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem022QueueUsingDeque;


public class TestProblem022QueueUsingDeque {
    @Test void t() { var q=new Problem022QueueUsingDeque.QueueX<Integer>(); q.enqueue(1); q.enqueue(2); assertEquals(1,q.dequeue().intValue()); }
}

