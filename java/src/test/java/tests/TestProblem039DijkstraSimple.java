package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem039DijkstraSimple;


import java.util.*;
public class TestProblem039DijkstraSimple {
    @Test void t(){ Map<String,List<String[]>> g=new HashMap<>(); g.put("A", List.of(new String[]{"B","1"})); g.put("B", List.of(new String[]{"C","2"})); g.put("C", List.of()); assertEquals(3, Problem039DijkstraSimple.dijkstra(g,"A").get("C")); }
}

