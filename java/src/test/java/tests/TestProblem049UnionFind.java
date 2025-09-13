package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem049UnionFind;


public class TestProblem049UnionFind {
    @Test void t(){ var u=new Problem049UnionFind.UF(3); u.union(0,1); assertEquals(u.find(0), u.find(1)); }
}

