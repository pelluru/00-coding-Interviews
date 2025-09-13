package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem040BfsGraph;


import java.util.*;
public class TestProblem040BfsGraph {
    @Test void t(){ Map<Integer,List<Integer>> g=Map.of(1,List.of(2,3),2,List.of(4),3,List.of(),4,List.of()); assertEquals(List.of(1,2,3,4), Problem040BfsGraph.bfs(g,1)); }
}

