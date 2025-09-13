package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem037LowestCommonAncestor;


public class TestProblem037LowestCommonAncestor {
    @Test void t(){ var r=new Problem037LowestCommonAncestor.Node(3); r.l=new Problem037LowestCommonAncestor.Node(5); r.r=new Problem037LowestCommonAncestor.Node(1); assertEquals(3, Problem037LowestCommonAncestor.lca(r,r.l,r.r).v); }
}

