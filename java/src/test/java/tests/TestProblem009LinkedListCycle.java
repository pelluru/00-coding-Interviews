package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem009LinkedListCycle;


public class TestProblem009LinkedListCycle {
    @Test void t() {
        var a=new Problem009LinkedListCycle.Node(1); var b=new Problem009LinkedListCycle.Node(2); var c=new Problem009LinkedListCycle.Node(3);
        a.next=b; b.next=c; c.next=b; assertTrue(Problem009LinkedListCycle.hasCycle(a));
    }
}

