package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem101FindKthSmallestBst;


public class TestProblem101FindKthSmallestBst {
    @Test void t(){
        var r=new Problem101FindKthSmallestBst.Node(2); r.l=new Problem101FindKthSmallestBst.Node(1); r.r=new Problem101FindKthSmallestBst.Node(3);
        assertEquals(2, Problem101FindKthSmallestBst.kthSmallest(r,2));
    }
}

