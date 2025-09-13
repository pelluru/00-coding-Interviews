package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem040DfsGraphRecursive;


import java.util.*;
public class TestProblem040DfsGraphRecursive {
    @Test void t(){
        Map<Integer,List<Integer>> g = Map.of(1,List.of(2,3), 2,List.of(4), 3,List.of(), 4,List.of());
        assertEquals(List.of(1,2,4,3), Problem040DfsGraphRecursive.dfs(g,1));
    }
}

