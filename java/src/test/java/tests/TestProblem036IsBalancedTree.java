package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem036IsBalancedTree;


public class TestProblem036IsBalancedTree {
    @Test void t(){ var r=new Problem036IsBalancedTree.Node(1); r.l=new Problem036IsBalancedTree.Node(2); assertTrue(Problem036IsBalancedTree.isBalanced(r)); }
}

